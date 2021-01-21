package corwhisk

import (
	"bufio"
	"encoding/base64"
	"github.com/ISE-SMILE/corral/internal/pkg/corbuild"
	"github.com/apache/openwhisk-client-go/whisk"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type WhiskClient struct {
	Client *whisk.Client
}

type WhiskFunctionConfig struct {
	Memory       int
	Timeout      int
	FunctionName string
}

var propsPath string

func init() {
	home, err := os.UserHomeDir()
	if err != nil {
		propsPath = filepath.Join(home, ".wskprops")
	} else {
		//best effort this will prop. work on unix and osx ;)
		propsPath = filepath.Join("~", ".wskprops")
	}
}

func readProps(in io.ReadCloser) map[string]string {
	defer in.Close()

	props := make(map[string]string)

	reader := bufio.NewScanner(in)

	for reader.Scan() {
		line := reader.Text()
		data := strings.SplitN(line, "=", 2)
		if len(data) < 2 {
			//This might leek user private data into a log...
			log.Errorf("could not read prop line %s", line)
		}
		props[data[0]] = data[1]
	}
	return props
}

func readEnviron() map[string]string {
	env := make(map[string]string)
	for _, e := range os.Environ() {
		data := strings.SplitN(e, "=", 2)
		if len(data) < 2 {
			//This might leek user private data into a log...
			log.Errorf("could not read prop line %s", e)
		}
		env[data[0]] = data[1]
	}
	return env
}

func whiskClient() (*whisk.Client, error) {
	// lets first look for .wskpros otherwise use viper?
	var host string
	var token string
	var namespace = "_"

	//1. check if wskprops exsist
	if stat, err := os.Stat(propsPath); err == nil {
		//2. attempt to read and parse props
		if props, err := os.Open(stat.Name()); err == nil {
			host, token, namespace = setAtuhFromProps(readProps(props))
		}
		//fallback try to check the env for token
	} else {
		host, token, namespace = setAtuhFromProps(readEnviron())
	}

	if token == "" {
		log.Warn("did not find a token for the whisk client!")
	}

	baseurl, _ := whisk.GetURLBase(host, "/api")
	clientConfig := &whisk.Config{
		AuthToken:        token,
		Namespace:        namespace,
		BaseURL:          baseurl,
		Version:          "v1",
		Insecure:         true,
		Host:             host,
		UserAgent:        "Golang/Smile cli",
		ApigwAccessToken: "Dummy Token",
	}

	client, err := whisk.NewClient(http.DefaultClient, clientConfig)
	if err != nil {
		return nil, err
	}

	return client, nil
}

//check props and env vars for relevant infomation ;)
func setAtuhFromProps(auth map[string]string) (string, string, string) {
	var host string
	var token string
	var namespace string
	if apihost, ok := auth["APIHOST"]; ok {
		host = apihost
	} else if apihost, ok := auth["__OW_API_HOST"]; ok {
		host = apihost
	}
	if apitoken, ok := auth["AUTH"]; ok {
		token = apitoken
	} else if apitoken, ok := auth["__OW_API_KEY"]; ok {
		token = apitoken
	}
	if apinamespace, ok := auth["NAMESPACE"]; ok {
		namespace = apinamespace
	} else if apinamespace, ok := auth["__OW_NAMESPACE"]; ok {
		namespace = apinamespace
	}
	return host, token, namespace
}

// MaxLambdaRetries is the number of times to try invoking a function
// before giving up and returning an error
const MaxRetries = 3

// NewLambdaClient initializes a new LambdaClient
func NewWhiskClient() *WhiskClient {
	client, err := whiskClient()
	if err != nil {
		log.Errorf("could not init whisk client - %+v", err)
	}
	return &WhiskClient{
		Client: client,
	}
}

func (l *WhiskClient) Invoke(name string, payload interface{}) (io.ReadCloser, error) {
	invoke, response, err := l.Client.Actions.Invoke(name, payload, true, true)
	if err != nil {
		log.Debugf("failed to invoke %s with %+v", name, payload)
		return nil, err
	}

	log.Debugf("invoked %s - %d", name, response.StatusCode)
	log.Debugf("%+v", invoke)

	return response.Body, err
}

// returns name and namespace based on function name
func getQualifiedName(functioname string) (string, string) {
	var namespace string
	var action string

	if strings.HasPrefix(functioname, "/") {
		parts := strings.Split(functioname, "/")
		namespace = parts[0]
		action = parts[1]
	} else {
		//no namespace set in string using _
		namespace = "_"
		action = functioname
	}

	return action, namespace

}

func (l WhiskClient) DeployFunction(conf WhiskFunctionConfig) error {

	actionName, namespace := getQualifiedName(conf.FunctionName)

	if conf.Memory == 0 {
		conf.Memory = 192
	}

	if conf.Timeout == 0 {
		conf.Timeout = int(time.Second * 30)
	}

	buildPackage, err := corbuild.BuildPackage()

	if err != nil {
		log.Debugf("failed to build package cause:+%v", err)
		return err
	}

	action := new(whisk.Action)

	action.Name = actionName
	action.Namespace = namespace
	action.Limits = &whisk.Limits{
		Timeout:     &conf.Timeout,
		Memory:      &conf.Memory,
		Logsize:     nil,
		Concurrency: nil,
	}
	payload := base64.StdEncoding.EncodeToString(buildPackage)
	action.Exec = &whisk.Exec{
		Kind:       "go:1.15",
		Code:       &payload,
		Main:       "Main",
		Components: nil,
		Binary:     nil,
	}
	action, _, err = l.Client.Actions.Insert(action, true)

	if err != nil {
		log.Debugf("failed to deploy %s cause %+v", conf.FunctionName, err)
		return err
	}

	log.Infof("deployed %s using [%s]", conf.FunctionName, action.Name)
	return nil
}

func (l *WhiskClient) DeleteFunction(name string) error {
	_, err := l.Client.Actions.Delete(name)
	return err
}
