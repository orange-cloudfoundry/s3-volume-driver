package params

type Mount struct {
	Uid             int
	Gid             int
	MountPoint      string
	MountOptions    map[string]string
	AccessKeyId     string
	Bucket          string
	SecretAccessKey string
	Endpoint        string
	Region          string
	RegionSet       bool
	StorageClass    string
	UseContentType  bool
	UseSSE          bool
	UseKMS          bool
	KMSKeyID        string
	ACL             string
	Subdomain       bool
}
