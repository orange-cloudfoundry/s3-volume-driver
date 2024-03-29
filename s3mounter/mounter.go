package main

import (
	"context"
	"github.com/hashicorp/go-multierror"
	"github.com/jacobsa/fuse"
	"github.com/kahing/goofys/api"
	"github.com/orange-cloudfoundry/s3-volume-driver/params"
	"os"
)

func mount(p params.Mount) (*fuse.MountedFileSystem, error) {
	err := os.MkdirAll(p.MountPoint, os.ModePerm)
	if err != nil {
		return nil, err
	}

	mountOptions := p.MountOptions
	if mountOptions == nil {
		mountOptions = make(map[string]string)
	}
	mountOptions["allow_other"] = ""
	_, mfs, err := goofys.Mount(context.Background(), p.Bucket, &goofys.Config{
		MountPoint: p.MountPoint,

		DirMode:      0777,
		FileMode:     0666,
		MountOptions: mountOptions,
		Uid:          uint32(p.Uid),
		Gid:          uint32(p.Gid),

		Endpoint:       p.Endpoint,
		AccessKey:      p.AccessKeyId,
		SecretKey:      p.SecretAccessKey,
		Region:         p.Region,
		RegionSet:      p.RegionSet,
		StorageClass:   p.StorageClass,
		UseContentType: p.UseContentType,
		UseSSE:         p.UseSSE,
		UseKMS:         p.UseKMS,
		ACL:            p.ACL,
		Subdomain:      p.Subdomain,
		KMSKeyID:       p.KMSKeyID,
	})
	if err != nil {
		result := err
		rm_err := os.Remove(p.MountPoint)
		if rm_err != nil {
			result = multierror.Append(result, rm_err)
		}
		return nil, result
	}
	return mfs, nil
}
