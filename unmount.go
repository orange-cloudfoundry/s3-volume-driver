package s3driver

import (
	"code.cloudfoundry.org/dockerdriver"
	"code.cloudfoundry.org/dockerdriver/driverhttp"
	"code.cloudfoundry.org/lager"
	"errors"
	"fmt"
	"github.com/kahing/goofys/api"
	"github.com/orange-cloudfoundry/s3-volume-driver/utils"
	"syscall"
)

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
		if err := d.unmount(driverhttp.EnvWithLogger(logger, env).Logger(), unmountRequest.Name, volume.Mountpoint, volume.Name); err != nil {
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

func (d *S3Driver) unmount(logger lager.Logger, name, mountPath, volumeName string) error {
	logger = logger.Session("unmount")
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

	err = goofys.TryUnmount(mountPath)
	if err != nil {
		logger.Error("unmount-failed", err)
		return fmt.Errorf("Error unmounting volume: %s", err.Error())
	}

	mounterPid := utils.MounterPid(d.mounterBoot.PidDir, volumeName)
	if mounterPid > 0 {
		err := syscall.Kill(mounterPid, syscall.SIGINT)
		if err != nil {
			logger.Error("sigint-mounter-failed", err)
		}
	}

	err = d.os.Remove(mountPath)
	if err != nil {
		logger.Error("remove-mountpoint-failed", err)
		return fmt.Errorf("Error removing mountpoint: %s", err.Error())
	}

	logger.Info("unmounted-volume")

	return nil
}
