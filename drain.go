package s3driver

import (
	"code.cloudfoundry.org/dockerdriver"
	"code.cloudfoundry.org/lager"
	"regexp"
)

func (d *S3Driver) Drain(env dockerdriver.Env) error {
	logger := env.Logger().Session("check-mounts")
	logger.Info("start")
	defer logger.Info("end")

	// flush any volumes that are still in our map
	for key, mount := range d.volumes {
		if mount.Mountpoint != "" && mount.MountCount > 0 {
			err := d.unmount(logger, mount.Name, mount.Mountpoint, mount.Name)
			if err != nil {
				logger.Error("drain-unmount-failed", err, lager.Data{"mount-name": mount.Name, "mount-point": mount.Mountpoint})
			}
		}
		delete(d.volumes, key)
	}

	d.Purge(env, d.mountPathRoot)
	d.removeState(env)
	return nil
}

func (d *S3Driver) Purge(env dockerdriver.Env, path string) {
	logger := env.Logger().Session("purge")
	logger.Info("purge-start")
	defer logger.Info("purge-end")

	mountPattern, err := regexp.Compile("^" + path + ".*$")
	if err != nil {
		logger.Error("unable-to-list-mounts", err)
		return
	}

	mounts, err := d.mountChecker.List(mountPattern)
	if err != nil {
		logger.Error("check-proc-mounts-failed", err, lager.Data{"path": path})
		return
	}

	logger.Info("mount-directory-list", lager.Data{"mounts": mounts})

	for _, mountDir := range mounts {
		_, err = d.invoker.Invoke(env, "umount", []string{"-l", "-f", mountDir})
		if err != nil {
			logger.Error("warning-umount-intermediate-failed", err)
		}

		logger.Info("unmount-successful", lager.Data{"path": mountDir})

		if err := d.os.Remove(mountDir); err != nil {
			logger.Error("purge-cannot-remove-directory", err, lager.Data{"name": mountDir, "path": path})
		}

		logger.Info("remove-directory-successful", lager.Data{"path": mountDir})

	}
}
