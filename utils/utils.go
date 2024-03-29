package utils

import (
	"log"
	"os/user"
	"strconv"
)

func CurrentUserAndGroup() (uid int, gid int) {
	// Ask for the current user.
	current, err := user.Current()
	if err != nil {
		panic(err)
	}
	return userAndGroup(current)
}

func VcapUserAndGroup() (uid int, gid int) {
	// Ask for the current user.
	vcap, err := user.Lookup("vcap")
	if err != nil {
		panic(err)
	}
	return userAndGroup(vcap)
}

func userAndGroup(u *user.User) (uid int, gid int) {
	// Parse UID.
	uid64, err := strconv.ParseInt(u.Uid, 10, 32)
	if err != nil {
		log.Fatalf("Parsing UID (%s): %v", u.Uid, err)
		return
	}

	// Parse GID.
	gid64, err := strconv.ParseInt(u.Gid, 10, 32)
	if err != nil {
		log.Fatalf("Parsing GID (%s): %v", u.Gid, err)
		return
	}

	uid = int(uid64)
	gid = int(gid64)
	return
}
