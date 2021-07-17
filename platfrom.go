package corral

/*
* platform abstraction to enable support for other FaaS Platforms than AWS Lambda
 */
type platform interface {
	Undeploy() error
	Deploy(driver *Driver) error
	Start(driver *Driver)
}
