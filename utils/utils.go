package utils

import (
	"fmt"
	"io/ioutil"
	"log"
	"os/user"
	"strconv"
	"strings"
)

func CurrentUserAndGroup() (uid int, gid int) {
	// Ask for the current user.
	vcap, err := user.Current()
	if err != nil {
		panic(err)
	}

	// Parse UID.
	uid64, err := strconv.ParseInt(vcap.Uid, 10, 32)
	if err != nil {
		log.Fatalf("Parsing UID (%s): %v", vcap.Uid, err)
		return
	}

	// Parse GID.
	gid64, err := strconv.ParseInt(vcap.Gid, 10, 32)
	if err != nil {
		log.Fatalf("Parsing GID (%s): %v", vcap.Gid, err)
		return
	}

	uid = int(uid64)
	gid = int(gid64)

	return
}

func MounterPidFileName(pidFolder, volumeName string) string {
	pidFolder = strings.TrimSuffix(pidFolder, "/")
	return fmt.Sprintf("%s/mounter-%s.pid", pidFolder, volumeName)
}

func MounterPid(pidFolder, volumeName string) int {
	p := MounterPidFileName(pidFolder, volumeName)
	b, err := ioutil.ReadFile(p)
	if err != nil {
		return -1
	}

	pidString := strings.TrimSpace(string(b))
	pid, err := strconv.Atoi(pidString)
	if err != nil {
		return -1
	}
	return pid
}

func MounterLogFile(logFolder, volumeName string) string {
	logFolder = strings.TrimSuffix(logFolder, "/")
	return fmt.Sprintf("%s/mounter-%s.log", logFolder, volumeName)
}
