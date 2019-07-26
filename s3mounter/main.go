package main

import (
	"context"
	"encoding/json"
	"github.com/kahing/goofys/api"
	"github.com/orange-cloudfoundry/s3-volume-driver/params"
	log "github.com/sirupsen/logrus"
	"os"
	"syscall"
	"time"
)

func init() {
	log.SetOutput(os.Stdout)
	goofys.GetLogger("main").SetOutput(os.Stdout)
	goofys.GetLogger("fuse").SetOutput(os.Stdout)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Mounter params is mandatory")
		syscall.Kill(os.Getppid(), syscall.SIGUSR2)
	}
	syscall.Umask(000)
	var mounterParams params.Mounter
	err := json.Unmarshal([]byte(os.Args[1]), &mounterParams)
	if err != nil {
		log.Fatal(err)
		syscall.Kill(os.Getppid(), syscall.SIGUSR2)
	}

	formatter := NewLogFormatter(mounterParams.VolumeName)
	log.SetFormatter(formatter)
	goofys.GetLogger("main").SetFormatter(formatter)
	goofys.GetLogger("fuse").SetFormatter(formatter)

	mfs, err := mount(mounterParams.MountParams)
	if err != nil {
		log.Fatal(err)
		syscall.Kill(os.Getppid(), syscall.SIGUSR2)
	}

	syscall.Kill(os.Getppid(), syscall.SIGUSR1)

	time.Sleep(1 * time.Second)
	log.Info("test")
	if err = mfs.Join(context.Background()); err != nil {
		log.Fatalf("Join: %v", err)
	}

}
