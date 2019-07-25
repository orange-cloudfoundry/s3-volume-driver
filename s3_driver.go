package s3driver

import (
	"code.cloudfoundry.org/dockerdriver"
	"code.cloudfoundry.org/dockerdriver/driverhttp"
	"code.cloudfoundry.org/dockerdriver/invoker"
	"code.cloudfoundry.org/goshims/filepathshim"
	"code.cloudfoundry.org/goshims/ioutilshim"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/goshims/timeshim"
	"code.cloudfoundry.org/lager"
	"context"
	"errors"
	"fmt"
	"github.com/cloudfoundry/volumedriver/mountchecker"
	"github.com/mitchellh/mapstructure"
	"sync"
	"time"
)

type S3VolumeInfo struct {
	ConnectionInfo ConnectionInfo
	mountError     string
	dockerdriver.VolumeInfo
}

type ConnectionInfo struct {
	AccessKeyId     string            `mapstructure:"access_key_id" json:"-"`
	Bucket          string            `mapstructure:"bucket"`
	SecretAccessKey string            `mapstructure:"secret_access_key" json:"-"`
	Endpoint        string            `mapstructure:"endpoint"`
	Region          string            `mapstructure:"region"`
	RegionSet       bool              `mapstructure:"region_set"`
	StorageClass    string            `mapstructure:"storage_class"`
	UseContentType  bool              `mapstructure:"use_content_type"`
	UseSSE          bool              `mapstructure:"use_sse"`
	UseKMS          bool              `mapstructure:"use_kms"`
	KMSKeyID        string            `mapstructure:"kms_key_id" json:"-"`
	ACL             string            `mapstructure:"acl"`
	Subdomain       bool              `mapstructure:"subdomain"`
	MountOptions    map[string]string `mapstructure:"mount_options"`
}

type OsHelper interface {
	Umask(mask int) (oldmask int)
}

type S3Driver struct {
	volumes       map[string]*S3VolumeInfo
	volumesLock   sync.RWMutex
	os            osshim.Os
	filepath      filepathshim.Filepath
	ioutil        ioutilshim.Ioutil
	time          timeshim.Time
	mountChecker  mountchecker.MountChecker
	mountPathRoot string
	osHelper      OsHelper
	invoker       invoker.Invoker
	mounterBoot   MounterBoot
}

type MounterBoot struct {
	MounterPath string
	LogDir      string
	PidDir      string
}

func NewS3Driver(
	logger lager.Logger,
	os osshim.Os,
	filepath filepathshim.Filepath,
	ioutil ioutilshim.Ioutil,
	time timeshim.Time,
	mountChecker mountchecker.MountChecker,
	mountPathRoot string,
	oshelper OsHelper,
	invoker invoker.Invoker,
	mounterBoot MounterBoot,
) *S3Driver {
	d := &S3Driver{
		volumes:       map[string]*S3VolumeInfo{},
		os:            os,
		filepath:      filepath,
		ioutil:        ioutil,
		time:          time,
		mountChecker:  mountChecker,
		mountPathRoot: mountPathRoot,
		osHelper:      oshelper,
		invoker:       invoker,
		mounterBoot:   mounterBoot,
	}

	ctx := context.TODO()
	env := driverhttp.NewHttpDriverEnv(logger, ctx)

	d.restoreState(env)

	return d
}

func (d S3Driver) Activate(env dockerdriver.Env) dockerdriver.ActivateResponse {
	return dockerdriver.ActivateResponse{
		Implements: []string{"VolumeDriver"},
	}
}

func (d S3Driver) Get(env dockerdriver.Env, getRequest dockerdriver.GetRequest) dockerdriver.GetResponse {
	volume, err := d.getVolume(env, getRequest.Name)
	if err != nil {
		return dockerdriver.GetResponse{Err: err.Error()}
	}

	return dockerdriver.GetResponse{
		Volume: dockerdriver.VolumeInfo{
			Name:       getRequest.Name,
			Mountpoint: volume.Mountpoint,
		},
	}
}

func (d S3Driver) getVolume(env dockerdriver.Env, volumeName string) (*S3VolumeInfo, error) {
	logger := env.Logger().Session("get-volume")
	d.volumesLock.RLock()
	defer d.volumesLock.RUnlock()

	if vol, ok := d.volumes[volumeName]; ok {
		logger.Info("getting-volume", lager.Data{"name": volumeName})
		return vol, nil
	}

	return &S3VolumeInfo{}, errors.New("Volume not found")
}

func (d S3Driver) List(env dockerdriver.Env) dockerdriver.ListResponse {
	d.volumesLock.RLock()
	defer d.volumesLock.RUnlock()

	listResponse := dockerdriver.ListResponse{
		Volumes: []dockerdriver.VolumeInfo{},
	}

	for _, volume := range d.volumes {
		if volume.MountCount == 0 {
			continue
		}
		listResponse.Volumes = append(listResponse.Volumes, volume.VolumeInfo)
	}
	listResponse.Err = ""
	return listResponse
}

func (d S3Driver) Path(env dockerdriver.Env, pathRequest dockerdriver.PathRequest) dockerdriver.PathResponse {
	logger := env.Logger().Session("path", lager.Data{"volume": pathRequest.Name})
	fmt.Println(pathRequest)
	if pathRequest.Name == "" {
		return dockerdriver.PathResponse{Err: "Missing mandatory 'volume_name'"}
	}

	vol, err := d.getVolume(driverhttp.EnvWithLogger(logger, env), pathRequest.Name)
	if err != nil {
		logger.Error("failed-no-such-volume-found", err, lager.Data{"mountpoint": vol.Mountpoint})

		return dockerdriver.PathResponse{Err: fmt.Sprintf("Volume '%s' not found", pathRequest.Name)}
	}

	if vol.Mountpoint == "" {
		errText := "Volume not previously mounted"
		logger.Error("failed-mountpoint-not-assigned", errors.New(errText))
		return dockerdriver.PathResponse{Err: errText}
	}

	return dockerdriver.PathResponse{Mountpoint: vol.Mountpoint}
}

func (d *S3Driver) Create(env dockerdriver.Env, createRequest dockerdriver.CreateRequest) dockerdriver.ErrorResponse {
	logger := env.Logger().Session("create")
	logger.Info("start")
	defer logger.Info("end")

	if createRequest.Name == "" {
		return dockerdriver.ErrorResponse{Err: "Missing mandatory 'volume_name'"}
	}

	var connInfo ConnectionInfo
	err := mapstructure.Decode(createRequest.Opts, &connInfo)
	if err != nil {
		return dockerdriver.ErrorResponse{Err: err.Error()}
	}

	if connInfo.Bucket == "" {
		return dockerdriver.ErrorResponse{Err: "Missing mandatory 'bucket' field in 'Opts'"}
	}
	if connInfo.AccessKeyId == "" {
		return dockerdriver.ErrorResponse{Err: "Missing mandatory 'access_key_id' field in 'Opts'"}
	}
	if connInfo.SecretAccessKey == "" {
		return dockerdriver.ErrorResponse{Err: "Missing mandatory 'secret_access_key' field in 'Opts'"}
	}

	existing, err := d.getVolume(driverhttp.EnvWithLogger(logger, env), createRequest.Name)

	if err != nil {
		logger.Info("creating-volume", lager.Data{"volume_name": createRequest.Name})
		logger.Info("with-opts", lager.Data{"opts": createRequest.Opts})

		volInfo := S3VolumeInfo{
			VolumeInfo:     dockerdriver.VolumeInfo{Name: createRequest.Name},
			ConnectionInfo: connInfo,
		}

		d.volumesLock.Lock()
		defer d.volumesLock.Unlock()

		d.volumes[createRequest.Name] = &volInfo
	} else {
		existing.ConnectionInfo = connInfo

		d.volumesLock.Lock()
		defer d.volumesLock.Unlock()

		d.volumes[createRequest.Name] = existing
	}

	err = d.persistState(driverhttp.EnvWithLogger(logger, env))
	if err != nil {
		logger.Error("persist-state-failed", err)
		return dockerdriver.ErrorResponse{Err: fmt.Sprintf("persist state failed when creating: %s", err.Error())}
	}

	return dockerdriver.ErrorResponse{}
}

func (d *S3Driver) Remove(env dockerdriver.Env, removeRequest dockerdriver.RemoveRequest) dockerdriver.ErrorResponse {
	logger := env.Logger().Session("remove", lager.Data{"volume": removeRequest})
	logger.Info("start")
	defer logger.Info("end")

	if removeRequest.Name == "" {
		return dockerdriver.ErrorResponse{Err: "Missing mandatory 'volume_name'"}
	}

	vol, err := d.getVolume(driverhttp.EnvWithLogger(logger, env), removeRequest.Name)

	if err != nil {
		logger.Error("warning-volume-removal", fmt.Errorf(fmt.Sprintf("Volume %s not found", removeRequest.Name)))
		return dockerdriver.ErrorResponse{}
	}

	if vol.Mountpoint != "" {
		if err := d.unmount(driverhttp.EnvWithLogger(logger, env).Logger(), removeRequest.Name, vol.Mountpoint, vol.Name); err != nil {
			return dockerdriver.ErrorResponse{Err: err.Error()}
		}
	}

	logger.Info("removing-volume", lager.Data{"name": removeRequest.Name})

	d.volumesLock.Lock()
	defer d.volumesLock.Unlock()
	delete(d.volumes, removeRequest.Name)

	if err := d.persistState(driverhttp.EnvWithLogger(logger, env)); err != nil {
		return dockerdriver.ErrorResponse{Err: fmt.Sprintf("failed to persist state when removing: %s", err.Error())}
	}

	return dockerdriver.ErrorResponse{}
}

func (d S3Driver) Capabilities(env dockerdriver.Env) dockerdriver.CapabilitiesResponse {
	return dockerdriver.CapabilitiesResponse{
		Capabilities: dockerdriver.CapabilityInfo{Scope: "local"},
	}
}

func (d *S3Driver) check(env dockerdriver.Env, name, mountPoint string) bool {
	ctx, cncl := context.WithDeadline(context.TODO(), time.Now().Add(time.Second*5))
	defer cncl()
	env = driverhttp.EnvWithContext(ctx, env)
	_, err := d.invoker.Invoke(env, "mountpoint", []string{"-q", mountPoint})

	if err != nil {
		// Note: Created volumes (with no mounts) will be removed
		//       since VolumeInfo.Mountpoint will be an empty string
		env.Logger().Info(fmt.Sprintf("unable to verify volume %s (%s)", name, err.Error()))
		return false
	}
	return true
}
