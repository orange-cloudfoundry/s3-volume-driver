package params

type Mounter struct {
	MountParams Mount
	PidFolder   string
	LogFolder   string
	VolumeName  string
}

type Mount struct {
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
