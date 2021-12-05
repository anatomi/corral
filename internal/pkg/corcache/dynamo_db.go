package corcache

import (
	"github.com/anatomi/corral/internal/pkg/corfs"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/lambda"
	"io"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
	"fmt"
	"bytes"
)

const TablePartitionKey = "CorralPartitionKey"
const TableSortKey = "CorralSortKey"
const ValueAttribute = "CorralValue"

type DynamoConfig struct {
	TableName					string
	TablePartitionKey			string
	TableSortKey				string
	ValueAttribute				string
	ReadCapacityUnits			int64
	WriteCapacityUnits			int64
}

type DynamoCache struct {
	Client	dynamodbiface.DynamoDBAPI
	Table	*dynamodb.TableDescription
	Config	*DynamoConfig
}

func NewDynamoCache() (*DynamoCache,error) {
	return &DynamoCache{
	},nil
}


func (D *DynamoCache) Init() error {
	// Create new table
	conf := DynamoConfig{}

	//conf.TableName = viper.GetString("dynamodbTableName")
	conf.TableName = "TestTable"
	conf.TablePartitionKey = TablePartitionKey
	conf.TableSortKey = TableSortKey
	conf.ValueAttribute = ValueAttribute
	/*conf.ReadCapacityUnits = viper.GetInt64("dynamodbRCP")
	conf.WriteCapacityUnits = viper.GetInt64("dynamodbWCP")*/
	conf.ReadCapacityUnits = int64(10)
	conf.WriteCapacityUnits = int64(10)

	D.Config = &conf

	err := D.InitDynamoTable(D.Config)
	if err != nil {
		return fmt.Errorf("Failed to create table, %+v",err)
	}

	return nil
}

func (D *DynamoCache) Deploy() error {
	if D.Client != nil{
		log.Debug("Dynamodb client was already initialized")
		return nil
	}

	return D.NewDynamoClient()
}

func (D *DynamoCache) Undeploy() error {
	if D.Client == nil {
		D.initClientOnLambda()
	}
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(D.Config.TableName),
	}
	
	_, err := D.Client.DeleteTable(input)
	return err
}

func (D *DynamoCache) ListFiles(pathGlob string) ([]corfs.FileInfo, error) {
	if D.Client == nil {
		D.initClientOnLambda()
	}

	pathGlob = strings.TrimSuffix(pathGlob, "*")
	filt := expression.Name(D.Config.TablePartitionKey).BeginsWith(pathGlob)
	proj := expression.NamesList(
		expression.Name(D.Config.TablePartitionKey),
		expression.Name(D.Config.TableSortKey),
		//expression.Name(D.Config.ValueAttribute),
	)
	
	expr, err := expression.NewBuilder().WithFilter(filt).WithProjection(proj).Build()
	if err != nil {
		fmt.Println(err)
	}
	input := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(D.Config.TableName),
		ReturnConsumedCapacity:    aws.String("TOTAL"),
	}
	result, err := D.Client.Scan(input)
	if err != nil {
		return nil,err
	}
	log.Infof("LIST Result: %#v", result)

	log.Infof("LIST ConsumedCapacity: %#v", result.ConsumedCapacity)
	files := make([]corfs.FileInfo, 0)
	if result.Items != nil {
		for _, file := range result.Items {
			fileInfo, err := D.Stat(*file[D.Config.TablePartitionKey].S)
			if err != nil {
				log.Error(err)
				continue
			}
			files = append(files, fileInfo)
		}
	}
	return files,nil
}

func (D *DynamoCache) Stat(path string) (corfs.FileInfo, error) {
	if D.Client == nil {
		D.initClientOnLambda()
	}

	input := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			D.Config.TablePartitionKey: {
				S: aws.String(path),
			},
			D.Config.TableSortKey: {
				S: aws.String(path),
			},
		},
		TableName: aws.String(D.Config.TableName),
		ReturnConsumedCapacity: aws.String("TOTAL"),
	}
	
	result, err := D.Client.GetItem(input)

	if err != nil {
		return corfs.FileInfo{},err
	}
	return corfs.FileInfo{
		Name: *result.Item[D.Config.TablePartitionKey].S,
		Size: int64(len([]byte(*result.Item[D.Config.ValueAttribute].S))),
	},nil
}

type bufferedDynamodbReader struct {
	*bytes.Buffer
}

func (b *bufferedDynamodbReader) Close() error {
	b.Buffer.Reset()
	return nil
}

func (D *DynamoCache) OpenReader(filePath string, startAt int64) (io.ReadCloser, error) {
	if D.Client == nil {
		D.initClientOnLambda()
	}
	
	input := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			D.Config.TablePartitionKey: {
				S: aws.String(filePath),
			},
			D.Config.TableSortKey: {
				S: aws.String(filePath),
			},
		},
		TableName: aws.String(D.Config.TableName),
		ReturnConsumedCapacity: aws.String("TOTAL"),
	}
	
	result, err := D.Client.GetItem(input)
	if err != nil {
		return nil, err
	} else {
		reader := &bufferedDynamodbReader{
			Buffer: bytes.NewBuffer([]byte(*result.Item[D.Config.ValueAttribute].S)),
		}
		if startAt > 0 {
			_ = reader.Next(int(startAt))
		}
		return reader,nil
	}
}

//Use a buffer but instead of writing all at once read the data in invervals
type bufferedDynamodbWriter struct {
	*bytes.Buffer
	key string
	client dynamodbiface.DynamoDBAPI
	config *DynamoConfig
}

func (b *bufferedDynamodbWriter) Close() error {
	bytes := b.Bytes()
	input := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			b.config.TablePartitionKey: {
				S: aws.String(b.key),
			},
			b.config.TableSortKey: {
				S: aws.String(b.key),
			},
			b.config.ValueAttribute: {
				S: aws.String(string(bytes)),
			},
		},
		ReturnConsumedCapacity: aws.String("TOTAL"),
		TableName:              aws.String(b.config.TableName),
	}
	
	result, err := b.client.PutItem(input)
	log.Infof("PUT ConsumedCapacity: %#v", result.ConsumedCapacity)
	return err
}

func (D *DynamoCache) newDynamodbWriter(key string,buffer []byte) *bufferedDynamodbWriter {
	if buffer == nil{
		buffer = []byte{}
	}

	return &bufferedDynamodbWriter{
		Buffer: bytes.NewBuffer(buffer),
		key:    key,
		client: D.Client,
		config: D.Config,
	}
}

func (D *DynamoCache) OpenWriter(filePath string) (io.WriteCloser, error) {
	if D.Client == nil {
		D.initClientOnLambda()
	}

	log.Infof("Config: %#v", D.Config)
	input := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			D.Config.TablePartitionKey: {
				S: aws.String(filePath),
			},
			D.Config.TableSortKey: {
				S: aws.String(filePath),
			},
		},
		TableName: aws.String(D.Config.TableName),
		ReturnConsumedCapacity: aws.String("TOTAL"),
	}
	
	result, err := D.Client.GetItem(input)
	log.Infof("OPEN WRITER Result: %#v", result)

	log.Infof("OPEN WRITER ConsumedCapacity: %#v", result.ConsumedCapacity)
	if err != nil {
		return nil, err
	}
	if result.Item != nil {
		return D.newDynamodbWriter(filePath,[]byte(*result.Item[D.Config.ValueAttribute].S)),nil
	} else {	
		return D.newDynamodbWriter(filePath,nil),nil

	}
}

func (D *DynamoCache) Delete(path string) error {
	if D.Client == nil {
		D.initClientOnLambda()
	}

	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			TablePartitionKey: {
				S: aws.String(path),
			},
			TableSortKey: {
				S: aws.String(path),
			},
		},
		TableName: aws.String(D.Config.TableName),
		ReturnConsumedCapacity: aws.String("TOTAL"),
	}

	result, err := D.Client.DeleteItem(input)
	log.Infof("DELETE ConsumedCapacity: %#v", result.ConsumedCapacity)

	return err
}

func (D *DynamoCache) Join(elem ...string) string {
	return strings.Join(elem,"")
}

func (D *DynamoCache) Split(path string) []string {
	return strings.Split(path,"")
}

func (D *DynamoCache) Flush(fs corfs.FileSystem) error {
	return nil
}

func (D *DynamoCache) Clear() error {
	if D.Client == nil {
		D.initClientOnLambda()
	}

	/*proj := expression.NamesList(
		expression.Name(D.Config.TablePartitionKey),
		expression.Name(D.Config.TableSortKey),
	)
	
	expr, err := expression.NewBuilder().WithProjection(proj).Build()
	if err != nil {
		fmt.Println(err)
	}*/
	input := &dynamodb.ScanInput{
		/*ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ProjectionExpression:      expr.Projection(),*/
		TableName:                 aws.String(D.Config.TableName),
	}
	result, err := D.Client.Scan(input)
	if err != nil {
		return err
	}
	for _, file := range result.Items {
		input := &dynamodb.DeleteItemInput{
			Key: map[string]*dynamodb.AttributeValue{
				D.Config.TablePartitionKey: {
					S: aws.String(*file[D.Config.TablePartitionKey].S),
				},
				D.Config.TableSortKey: {
					S: aws.String(*file[D.Config.TableSortKey].S),
				},
			},
			TableName: aws.String(D.Config.TableName),
		}
		
		_, err := D.Client.DeleteItem(input)
		if err != nil {
			return err
		}
	}
	return nil
}

func (D *DynamoCache) FunctionInjector() CacheConfigIncector {
	return &DynamoCacheConfigInjector{D}
}

type DynamoCacheConfigInjector struct {
	system *DynamoCache
}

func (d *DynamoCacheConfigInjector) CacheSystem() CacheSystem {
	return d.system
}


func (d *DynamoCacheConfigInjector) ConfigureLambda(functionConfig *lambda.CreateFunctionInput) error {	

	if d.system == nil {
		return fmt.Errorf("Dynamodb Reference missing")
	}

	if d.system.Config == nil {
		return fmt.Errorf("Cache Config not availible")
	}

	functionConfig.Environment.Variables["DYNAMO_TABLE_NAME"] = &d.system.Config.TableName
	functionConfig.Environment.Variables["DYNAMO_PARTITION_KEY"] = &d.system.Config.TablePartitionKey
	functionConfig.Environment.Variables["DYNAMO_SORT_KEY"] = &d.system.Config.TableSortKey
	functionConfig.Environment.Variables["DYNAMO_VALUE_ATTR"] = &d.system.Config.ValueAttribute
	
	log.Infof("ADD env variables")
	return nil
}

func (D *DynamoCache) NewDynamoClient() error {
	os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	sess := session.Must(session.NewSession())
	D.Client = dynamodb.New(sess)
	log.Infof("DynamoDB client initialised")

	return nil
}

func (D *DynamoCache) InitDynamoTable(config *DynamoConfig) error {
	table, err := D.Client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(config.TableName),
	})
	if err != nil {
		log.Infof("table does not exist")
		aerr, _ := err.(awserr.Error);
		if aerr.Code() == dynamodb.ErrCodeResourceNotFoundException {
			// create new Table
			createTableInput := &dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{
					{
						AttributeName: aws.String(config.TablePartitionKey),
						AttributeType: aws.String("S"),
					},
					{
						AttributeName: aws.String(config.TableSortKey),
						AttributeType: aws.String("S"),
					},
				},
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String(config.TablePartitionKey),
						KeyType:       aws.String("HASH"),
					},
					{
						AttributeName: aws.String(config.TableSortKey),
						KeyType:       aws.String("RANGE"),
					},
				},
				ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(20),
					WriteCapacityUnits: aws.Int64(20),
				},
				TableName: aws.String(config.TableName),
			}
			result, err := D.Client.CreateTable(createTableInput)
			if err != nil {
				return err
			} else {
				D.Table = result.TableDescription
			}
		}
	} else {
		D.Table = table.Table
	}
	log.Infof("Status: %s", *D.Table.TableStatus)
		// wait until file system is avaiable
	for *D.Table.TableStatus != "ACTIVE" {
		table, err := D.Client.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: D.Table.TableName,
		})
		if err != nil {
			return err
		}
		
		D.Table = table.Table
	}
	
	log.Infof("Table is avaiable")
	return nil
}

func (D *DynamoCache) initClientOnLambda() error {
	conf := DynamoConfig{}

	if tableName := os.Getenv("DYNAMO_TABLE_NAME"); tableName != "" {
		conf.TableName = tableName
	} else {
		panic("Could not find TableName in lambda")
	}
	if tablePK := os.Getenv("DYNAMO_PARTITION_KEY"); tablePK != "" {
		conf.TablePartitionKey = tablePK
	} else {
		panic("Could not find TablePartitionKey in lambda")
	}
	if tableSK := os.Getenv("DYNAMO_SORT_KEY"); tableSK != "" {
		conf.TableSortKey = tableSK
	} else {
		panic("Could not find TableSortKey in lambda")
	}
	if tableVA := os.Getenv("DYNAMO_VALUE_ATTR"); tableVA != "" {
		conf.ValueAttribute = tableVA
	} else {
		panic("Could not find TableValueAttr in lambda")
	}

	D.Config = &conf
	D.NewDynamoClient()
	return nil
}