package s3driver

import (
	"log"
	"os/user"
	"strconv"
)

func vcapUserAndGroup() (uid int, gid int) {
	vcap, err := user.Lookup("vcap")
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
