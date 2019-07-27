# S3-volume-driver

This repo contains code to create and mount a s3 based filesystem for cloud foundry applications.

For the documentation please go to the repo of the bosh release associated: https://github.com/orange-cloudfoundry/s3-volume-release

Under the hood, it uses [goofys](https://github.com/kahing/goofys) and fuse for mounting.