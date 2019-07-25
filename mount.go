package s3driver

import (
	"code.cloudfoundry.org/dockerdriver"
	"code.cloudfoundry.org/dockerdriver/driverhttp"
	"code.cloudfoundry.org/lager"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kahing/goofys/api"
	"os"
	"path/filepath"
	"time"
)

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
			connInfo = volume.ConnectionInfo
		}

		volume.Mountpoint = mountPath
		volume.MountCount++

		logger.Info("volume-ref-count-incremented", lager.Data{"name": volume.Name, "count": volume.MountCount})

		if err := d.persistState(driverhttp.EnvWithLogger(logger, env)); err != nil {
			logger.Error("persist-state-failed", err)
			return dockerdriver.MountResponse{Err: fmt.Sprintf("persist state failed when mounting: %s", err.Error())}
		}

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

	}

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
				if err := d.mount(driverhttp.EnvWithLogger(logger, env), volume.ConnectionInfo, mountPath); err != nil {
					logger.Error("remount-volume-failed", err)
					return dockerdriver.MountResponse{Err: fmt.Sprintf("Error remounting volume: %s", err.Error())}
				}
			}
			return dockerdriver.MountResponse{Mountpoint: volume.Mountpoint}
		}
	}()
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

	uid, gid := currentUserAndGroup()

	if _, err := os.Stat(mountPath); os.IsNotExist(err) {
		orig := d.osHelper.Umask(000)
		defer d.osHelper.Umask(orig)

		err := d.os.MkdirAll(mountPath, os.ModePerm)
		if err != nil {
			logger.Error("create-mountdir-failed", err)
			return err
		}

		err = d.os.Chown(mountPath, uid, gid)
		if err != nil {
			logger.Error("chown-mountdir-failed", err)
			return err
		}
	}

	_, mfs, err := goofys.Mount(context.Background(), connInfo.Bucket, &goofys.Config{
		MountPoint: mountPath,
		DirMode:    0755,
		FileMode:   0644,
		MountOptions: map[string]string{
			"allow_other": "",
		},
		Uid: uint32(uid),
		Gid: uint32(gid),

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
	mfs.Join()
	if err != nil {
		logger.Error("mount-failed: ", err)
		rm_err := d.os.Remove(mountPath)
		if rm_err != nil {
			logger.Error("mountpoint-remove-failed", rm_err, lager.Data{"mount-path": mountPath})
		}
	}
	return err
}
