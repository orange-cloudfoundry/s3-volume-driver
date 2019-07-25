package s3driver

import (
	"code.cloudfoundry.org/dockerdriver"
	"code.cloudfoundry.org/dockerdriver/driverhttp"
	"code.cloudfoundry.org/lager"
	"encoding/json"
	"os"
	"path/filepath"
)

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
	d.volumes = state
	d.volumesLock.Unlock()

	logger.Info("remount-volumes-from-state", lager.Data{"state": state})
	for _, volume := range d.volumes {
		if volume.MountCount == 0 {
			continue
		}
		if volume.ConnectionInfo.Bucket == "" {
			d.volumesLock.Lock()
			delete(d.volumes, volume.Name)
			os.Remove(volume.Mountpoint)
			d.volumesLock.Unlock()
			continue
		}
	}
	d.volumesLock.Lock()
	defer d.volumesLock.Unlock()
	err = d.persistState(driverhttp.EnvWithLogger(logger, env))
	if err != nil {
		logger.Error("persist-state-failed", err)
	}
}

func (d *S3Driver) removeState(env dockerdriver.Env) {
	logger := env.Logger().Session("remove-state")
	logger.Info("start")
	defer logger.Info("end")
	os.Remove(d.mountPath(env, "driver-state.json"))
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
