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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cloudfoundry/volumedriver/mountchecker"
	"github.com/jacobsa/fuse"
	"github.com/kahing/goofys/api"
	"github.com/mitchellh/mapstructure"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type S3VolumeInfo struct {
	ConnectionInfo ConnectionInfo `json:"-"` // don't store opts
	wg             sync.WaitGroup
	mountError     string
	dockerdriver.VolumeInfo
}

type ConnectionInfo struct {
	AccessKeyId     string `mapstructure:"access_key_id"`
	Bucket          string `mapstructure:"bucket"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	Host            string `mapstructure:"host"`
	Region          string `mapstructure:"region"`
	RegionSet       bool   `mapstructure:"region_set"`
	StorageClass    string `mapstructure:"storage_class"`
	UseContentType  bool   `mapstructure:"use_content_type"`
	UseSSE          bool   `mapstructure:"use_sse"`
	UseKMS          bool   `mapstructure:"use_kms"`
	KMSKeyID        string `mapstructure:"kmskey_id"`
	ACL             string `mapstructure:"acl"`
	Subdomain       bool   `mapstructure:"subdomain"`
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
		if err := d.unmount(driverhttp.EnvWithLogger(logger, env), removeRequest.Name, vol.Mountpoint); err != nil {
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

func (d *S3Driver) Mount(env dockerdriver.Env, mountRequest dockerdriver.MountRequest) dockerdriver.MountResponse {
	logger := env.Logger().Session("mount", lager.Data{"volume": mountRequest.Name})
	logger.Info("start")
	defer logger.Info("end")

	if mountRequest.Name == "" {
		return dockerdriver.MountResponse{Err: "Missing mandatory 'volume_name'"}
	}

	var doMount bool
	var connInfo ConnectionInfo
	var mountPath string
	var wg *sync.WaitGroup

	ret := func() dockerdriver.MountResponse {

		d.volumesLock.Lock()
		defer d.volumesLock.Unlock()

		volume := d.volumes[mountRequest.Name]
		if volume == nil {
			return dockerdriver.MountResponse{Err: fmt.Sprintf("Volume '%s' must be created before being mounted", mountRequest.Name)}
		}

		mountPath = d.mountPath(driverhttp.EnvWithLogger(logger, env), volume.Name)

		logger.Info("mounting-volume", lager.Data{"id": volume.Name, "mountpoint": mountPath})
		logger.Info("mount-source", lager.Data{"bucket": volume.ConnectionInfo.Bucket})

		if volume.MountCount < 1 {
			doMount = true
			volume.wg.Add(1)
			connInfo = volume.ConnectionInfo
		}

		volume.Mountpoint = mountPath
		volume.MountCount++

		logger.Info("volume-ref-count-incremented", lager.Data{"name": volume.Name, "count": volume.MountCount})

		if err := d.persistState(driverhttp.EnvWithLogger(logger, env)); err != nil {
			logger.Error("persist-state-failed", err)
			return dockerdriver.MountResponse{Err: fmt.Sprintf("persist state failed when mounting: %s", err.Error())}
		}

		wg = &volume.wg
		return dockerdriver.MountResponse{Mountpoint: volume.Mountpoint}
	}()

	if ret.Err != "" {
		return ret
	}

	if doMount {
		mountStartTime := d.time.Now()

		err := d.mount(driverhttp.EnvWithLogger(logger, env), connInfo, mountPath)

		mountEndTime := d.time.Now()
		mountDuration := mountEndTime.Sub(mountStartTime)
		if mountDuration > 8*time.Second {
			logger.Error("mount-duration-too-high", nil, lager.Data{"mount-duration-in-second": mountDuration / time.Second, "warning": "This may result in container creation failure!"})
		}

		func() {
			d.volumesLock.Lock()
			defer d.volumesLock.Unlock()

			volume := d.volumes[mountRequest.Name]
			if volume == nil {
				ret = dockerdriver.MountResponse{Err: fmt.Sprintf("Volume '%s' not found", mountRequest.Name)}
			} else if err != nil {
				if _, ok := err.(dockerdriver.SafeError); ok {
					errBytes, m_err := json.Marshal(err)
					if m_err != nil {
						logger.Error("failed-to-marshal-safeerror", m_err)
						volume.mountError = err.Error()
					}
					volume.mountError = string(errBytes)
				} else {
					volume.mountError = err.Error()
				}
			}
		}()

		wg.Done()
	}

	wg.Wait()

	return func() dockerdriver.MountResponse {
		d.volumesLock.Lock()
		defer d.volumesLock.Unlock()

		volume := d.volumes[mountRequest.Name]
		if volume == nil {
			return dockerdriver.MountResponse{Err: fmt.Sprintf("Volume '%s' not found", mountRequest.Name)}
		} else if volume.mountError != "" {
			return dockerdriver.MountResponse{Err: volume.mountError}
		} else {
			// Check the volume to make sure it's still mounted before handing it out again.
			if !doMount && !d.check(driverhttp.EnvWithLogger(logger, env), volume.Name, volume.Mountpoint) {
				wg.Add(1)
				defer wg.Done()
				if err := d.mount(driverhttp.EnvWithLogger(logger, env), volume.ConnectionInfo, mountPath); err != nil {
					logger.Error("remount-volume-failed", err)
					return dockerdriver.MountResponse{Err: fmt.Sprintf("Error remounting volume: %s", err.Error())}
				}
			}
			return dockerdriver.MountResponse{Mountpoint: volume.Mountpoint}
		}
	}()
}

func (d *S3Driver) Unmount(env dockerdriver.Env, unmountRequest dockerdriver.UnmountRequest) dockerdriver.ErrorResponse {
	logger := env.Logger().Session("unmount", lager.Data{"volume": unmountRequest.Name})

	if unmountRequest.Name == "" {
		return dockerdriver.ErrorResponse{Err: "Missing mandatory 'volume_name'"}
	}

	d.volumesLock.Lock()
	defer d.volumesLock.Unlock()

	volume, ok := d.volumes[unmountRequest.Name]
	if !ok {
		logger.Error("failed-no-such-volume-found", fmt.Errorf("could not find volume %s", unmountRequest.Name))

		return dockerdriver.ErrorResponse{Err: fmt.Sprintf("Volume '%s' not found", unmountRequest.Name)}
	}

	if volume.Mountpoint == "" {
		errText := "Volume not previously mounted"
		logger.Error("failed-mountpoint-not-assigned", errors.New(errText))
		return dockerdriver.ErrorResponse{Err: errText}
	}

	if volume.MountCount == 1 {
		if err := d.unmount(driverhttp.EnvWithLogger(logger, env), unmountRequest.Name, volume.Mountpoint); err != nil {
			return dockerdriver.ErrorResponse{Err: err.Error()}
		}
	}

	volume.MountCount--
	logger.Info("volume-ref-count-decremented", lager.Data{"name": volume.Name, "count": volume.MountCount})

	if volume.MountCount < 1 {
		delete(d.volumes, unmountRequest.Name)
	}

	if err := d.persistState(driverhttp.EnvWithLogger(logger, env)); err != nil {
		return dockerdriver.ErrorResponse{Err: fmt.Sprintf("failed to persist state when unmounting: %s", err.Error())}
	}

	return dockerdriver.ErrorResponse{}
}

func (d S3Driver) Capabilities(env dockerdriver.Env) dockerdriver.CapabilitiesResponse {
	return dockerdriver.CapabilitiesResponse{
		Capabilities: dockerdriver.CapabilityInfo{Scope: "local"},
	}
}

func (d *S3Driver) restoreState(env dockerdriver.Env) {
	logger := env.Logger().Session("restore-state")
	logger.Info("start")
	defer logger.Info("end")

	stateFile := filepath.Join(d.mountPathRoot, "driver-state.json")

	stateData, err := d.ioutil.ReadFile(stateFile)
	if err != nil {
		logger.Info("failed-to-read-state-file", lager.Data{"err": err, "stateFile": stateFile})
		return
	}

	state := map[string]*S3VolumeInfo{}
	err = json.Unmarshal(stateData, &state)

	logger.Info("state", lager.Data{"state": state})

	if err != nil {
		logger.Error("failed-to-unmarshall-state", err, lager.Data{"stateFile": stateFile})
		return
	}
	logger.Info("state-restored", lager.Data{"state-file": stateFile})

	d.volumesLock.Lock()
	defer d.volumesLock.Unlock()
	d.volumes = state
}

func (d *S3Driver) persistState(env dockerdriver.Env) error {
	logger := env.Logger().Session("persist-state")
	logger.Info("start")
	defer logger.Info("end")

	orig := d.osHelper.Umask(000)
	defer d.osHelper.Umask(orig)

	stateFile := d.mountPath(env, "driver-state.json")

	stateData, err := json.Marshal(d.volumes)
	if err != nil {
		logger.Error("failed-to-marshall-state", err)
		return err
	}

	err = d.ioutil.WriteFile(stateFile, stateData, os.ModePerm)
	if err != nil {
		logger.Error("failed-to-write-state-file", err, lager.Data{"stateFile": stateFile})
		return err
	}

	logger.Debug("state-saved", lager.Data{"state-file": stateFile})
	return nil
}

func (d *S3Driver) mountPath(env dockerdriver.Env, volumeId string) string {
	logger := env.Logger().Session("mount-path")
	orig := d.osHelper.Umask(000)
	defer d.osHelper.Umask(orig)

	dir, err := d.filepath.Abs(d.mountPathRoot)
	if err != nil {
		logger.Fatal("abs-failed", err)
	}

	if err := d.os.MkdirAll(dir, os.ModePerm); err != nil {
		logger.Fatal("mkdir-rootpath-failed", err)
	}

	return filepath.Join(dir, volumeId)
}

func (d *S3Driver) unmount(env dockerdriver.Env, name string, mountPath string) error {
	logger := env.Logger().Session("unmount")
	logger.Info("start")
	defer logger.Info("end")

	exists, err := d.mountChecker.Exists(mountPath)
	if err != nil {
		logger.Error("failed-proc-mounts-check", err, lager.Data{"mountpoint": mountPath})
		return err
	}

	if !exists {
		err := d.os.Remove(mountPath)
		if err != nil {
			errText := fmt.Sprintf("Volume %s does not exist (path: %s) and unable to remove mount directory", name, mountPath)
			logger.Info("mountpoint-not-found", lager.Data{"msg": errText})
			return errors.New(errText)
		}

		errText := fmt.Sprintf("Volume %s does not exist (path: %s)", name, mountPath)
		logger.Info("mountpoint-not-found", lager.Data{"msg": errText})
		return errors.New(errText)
	}

	logger.Info("unmount-volume-folder", lager.Data{"mountpath": mountPath})

	err = fuse.Unmount(mountPath)
	if err != nil {
		logger.Error("unmount-failed", err)
		return fmt.Errorf("Error unmounting volume: %s", err.Error())
	}
	err = d.os.Remove(mountPath)
	if err != nil {
		logger.Error("remove-mountpoint-failed", err)
		return fmt.Errorf("Error removing mountpoint: %s", err.Error())
	}

	logger.Info("unmounted-volume")

	return nil
}

func (d *S3Driver) mount(env dockerdriver.Env, connInfo ConnectionInfo, mountPath string) error {
	logger := env.Logger().Session("mount", lager.Data{"bucket": connInfo.Bucket, "target": mountPath})
	logger.Info("start")
	defer logger.Info("end")

	if connInfo.Bucket == "" {
		err := errors.New("no source information")
		logger.Error("unable-to-extract-source", err)
		return err
	}
	if connInfo.AccessKeyId == "" {
		err := errors.New("no access key id")
		logger.Error("unable-to-extract-access-key-id", err)
		return err
	}
	if connInfo.SecretAccessKey == "" {
		err := errors.New("no secret access key")
		logger.Error("unable-to-extract-secret-access-key", err)
		return err
	}

	orig := d.osHelper.Umask(000)
	defer d.osHelper.Umask(orig)

	err := d.os.MkdirAll(mountPath, os.ModePerm)
	if err != nil {
		logger.Error("create-mountdir-failed", err)
		return err
	}

	uid, gid := currentUserAndGroup()
	_, _, err = goofys.Mount(context.Background(), connInfo.Bucket, &goofys.Config{
		MountPoint: mountPath,
		DirMode:    0755,
		FileMode:   0644,
		Uid:        uint32(uid),
		Gid:        uint32(gid),

		Endpoint:       connInfo.Host,
		AccessKey:      connInfo.AccessKeyId,
		SecretKey:      connInfo.SecretAccessKey,
		Region:         connInfo.Region,
		RegionSet:      connInfo.RegionSet,
		StorageClass:   connInfo.StorageClass,
		UseContentType: connInfo.UseContentType,
		UseSSE:         connInfo.UseSSE,
		UseKMS:         connInfo.UseKMS,
		ACL:            connInfo.ACL,
		Subdomain:      connInfo.Subdomain,
	})
	if err != nil {
		logger.Error("mount-failed: ", err)
		rm_err := d.os.Remove(mountPath)
		if rm_err != nil {
			logger.Error("mountpoint-remove-failed", rm_err, lager.Data{"mount-path": mountPath})
		}
	}
	return err
}

func (m *S3Driver) check(env dockerdriver.Env, name, mountPoint string) bool {
	ctx, cncl := context.WithDeadline(context.TODO(), time.Now().Add(time.Second*5))
	defer cncl()
	env = driverhttp.EnvWithContext(ctx, env)
	_, err := m.invoker.Invoke(env, "mountpoint", []string{"-q", mountPoint})

	if err != nil {
		// Note: Created volumes (with no mounts) will be removed
		//       since VolumeInfo.Mountpoint will be an empty string
		env.Logger().Info(fmt.Sprintf("unable to verify volume %s (%s)", name, err.Error()))
		return false
	}
	return true
}
