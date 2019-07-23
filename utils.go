package s3driver

import (
	"log"
	"os/user"
	"strconv"
)

func currentUserAndGroup() (uid int, gid int) {
	// Ask for the current user.
	user, err := user.Current()
	if err != nil {
		panic(err)
	}

	// Parse UID.
	uid64, err := strconv.ParseInt(user.Uid, 10, 32)
	if err != nil {
		log.Fatalf("Parsing UID (%s): %v", user.Uid, err)
		return
	}

	// Parse GID.
	gid64, err := strconv.ParseInt(user.Gid, 10, 32)
	if err != nil {
		log.Fatalf("Parsing GID (%s): %v", user.Gid, err)
		return
	}

	uid = int(uid64)
	gid = int(gid64)

	return
}
