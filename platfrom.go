package corral

/*
* platform abstraction to enable support for other FaaS Platforms than AWS Lambda
 */
type platform interface {
	Undeploy()
	Deploy()
	Start(driver *Driver)
}
