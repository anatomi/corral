package corcache

import (
	"bytes"
	"context"
	"fmt"
	"github.com/mitchellh/go-homedir"
	helmClient "github.com/mittwald/go-helm-client"
	"gopkg.in/yaml.v2"
	"helm.sh/helm/v3/pkg/repo"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"path/filepath"
	"strings"
	"time"
)

//TODO:
var RedisKubernetesChartRepo = repo.Entry{
	Name:                  "groundhog2k",
	URL:                   "https://groundhog2k.github.io/helm-charts/",
}
const RedisKubernetesChart = "groundhog2k/redis"
const RedisKubernetesDeploymentName = "corral-redis"

type KubernetesRedisDeploymentStrategy struct {
	Namespace  string
	StorageClass string
	NodePort *int

	deploymentName string

	helmClient helmClient.Client

}

type kubernetesRedisConfig struct {
	Service struct {
		Type     string `yaml:"type,omitempty"`
		NodePort *int   `yaml:"nodePort,omitempty"`
	} `yaml:"service,omitempty"`

	Storage struct {
		Class         string `yaml:"className,omitempty"`
		RequestedSize string  `yaml:"requestedSize,omitempty"`
	} `yaml:"storage,omitempty"`

	Resources struct {
		Limits struct {
			Memory string `yaml:"memory,omitempty"`
		} `yaml:"limits,omitempty"`
	} `yaml:"resources,omitempty"`
}

func (k *KubernetesRedisDeploymentStrategy) config() (string, error) {
	conf := &kubernetesRedisConfig{}

	conf.Resources.Limits.Memory = "512Mi"
	conf.Storage.Class = k.StorageClass

	if k.NodePort != nil {
		conf.Service.Type = "NodePort"
		conf.Service.NodePort = k.NodePort
	}

	buf := bytes.NewBuffer(make([]byte,0))
	enc := yaml.NewEncoder(buf)
	err := enc.Encode(conf)
	if err != nil {
		return "", err
	}

	return string(buf.Bytes()),nil
}


func (k *KubernetesRedisDeploymentStrategy) Deploy() (*ClientConfig, error) {
	ctx := context.Background()
	cli,err := k.clinet()
	if err != nil {
		return nil,fmt.Errorf("failed to create helm client, %+v",err)
	}

	err = cli.AddOrUpdateChartRepo(RedisKubernetesChartRepo)
	if err != nil {
		return nil,fmt.Errorf("failed to add repo, %+v",err)
	}

	conf,err := k.config()
	if err != nil{
		return nil,fmt.Errorf("failed to generate deployment config, %+v",err)
	}

	k8s,err := readKubernetesConf()

	if err != nil || k8s == nil {
		return nil,fmt.Errorf("failed to resolve k8s env %+v",err)
	}

	if k.Namespace == "" {
		k.Namespace = k8s.namespace
	}

	chartSpec := helmClient.ChartSpec{
		ReleaseName: RedisKubernetesDeploymentName,
		ChartName:   RedisKubernetesChart,
		Namespace:   k.Namespace,
		ValuesYaml:  conf,
		Wait:        true,
		Timeout:     5*time.Minute,
	}


	_,err = cli.InstallOrUpgradeChart(ctx,&chartSpec)
	if err != nil{
		return nil,fmt.Errorf("failed to deploy, %+v",err)
	}

	redis_conf := ClientConfig{
		Addrs:          nil,
	}

	if k.NodePort != nil {
		redis_conf.Addrs = []string{
			fmt.Sprintf("%s:%d",k8s.master,*k.NodePort),
		}
	} else {
		//well that looks terible..
		get, err := k8s.client.CoreV1().Services(k8s.namespace).Get(ctx, fmt.Sprintf("%s-master", RedisKubernetesDeploymentName), v1.GetOptions{})
		if err != nil {
			return nil, err
		}

		redis_conf.Addrs = []string{
			fmt.Sprintf("%s:%d",get.Spec.ClusterIP,6379),
		}
	}



	return &redis_conf, nil

}

func (k *KubernetesRedisDeploymentStrategy) Undeploy() error {
	cli,err := k.clinet()
	if err != nil {
		return fmt.Errorf("failed to create helm client, %+v",err)
	}

	k8s,err := readKubernetesConf()
	if err != nil || k8s == nil {
		return fmt.Errorf("failed to resolve k8s env %+v",err)
	}

	if k.Namespace == "" {
		k.Namespace = k8s.namespace
	}

	chartSpec := helmClient.ChartSpec{
		ReleaseName: RedisKubernetesDeploymentName,
		ChartName:   RedisKubernetesChart,
		Namespace:   k.Namespace,
		Wait:        true,
	}

	return cli.UninstallRelease(&chartSpec)
}

func (k *KubernetesRedisDeploymentStrategy) clinet() (helmClient.Client, error) {
	if k.helmClient != nil {
		return k.helmClient,nil
	} else {
		cache := filepath.Join(os.TempDir(),".helmcache")
		repos := filepath.Join(os.TempDir(),".helmrepo")


		cli,err := helmClient.New(&helmClient.Options{
			RepositoryConfig: repos,
			RepositoryCache:  cache,
			Debug:            false,
			Linting:          false,
		})
		if err != nil {
			return nil,err
		}
		k.helmClient = cli
		return cli,nil
	}
}

type kubernetesEnv struct {

	namespace string
	master    string
	client    *kubernetes.Clientset

}

func readKubernetesConf() (*kubernetesEnv,error) {
	path := os.Getenv("KUBECONFIG")
	if path == "" {
		p, err := homedir.Expand("~/.kube/config")
		if err != nil {
			return  nil, err
		}
		path = p
	}

	kubeConfig, err := clientcmd.BuildConfigFromFlags("",path)
	if err != nil {
		return  nil, err
	}


	clientset,err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return  nil, err
	}

	host := strings.Split(kubeConfig.Host[len("https://"):],":")[0]

	return &kubernetesEnv{
		master:    host,
		client:    clientset,
	},nil


}

