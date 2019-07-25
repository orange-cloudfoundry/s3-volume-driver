package main

import (
	"context"
	"encoding/json"
	"github.com/orange-cloudfoundry/s3-volume-driver/params"
	"github.com/orange-cloudfoundry/s3-volume-driver/utils"
	"github.com/sevlyar/go-daemon"
	log "github.com/sirupsen/logrus"
	"os"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Mounter params is mandatory")
	}

	var mounterParams params.Mounter
	err := json.Unmarshal([]byte(os.Args[1]), &mounterParams)
	if err != nil {
		log.Fatal(err)
	}

	cntxt := &daemon.Context{
		PidFileName: utils.MounterPidFileName(mounterParams.PidFolder, mounterParams.VolumeName),
		PidFilePerm: 0644,
		LogFileName: utils.MounterLogFile(mounterParams.LogFolder, mounterParams.VolumeName),
		LogFilePerm: 0640,
		Umask:       027,
	}

	d, err := cntxt.Reborn()
	if err != nil {
		log.Fatal("Unable to run: ", err)
	}
	if d != nil {
		return
	}
	defer cntxt.Release()

	mfs, err := mount(mounterParams.MountParams)
	if err != nil {
		log.Fatal(err)
	}

	if err = mfs.Join(context.Background()); err != nil {
		log.Fatalf("Join: %v", err)
	}

}
