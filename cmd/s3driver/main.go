package main

import (
	"code.cloudfoundry.org/goshims/timeshim"
	"encoding/json"
	"flag"
	"github.com/cloudfoundry/volumedriver/mountchecker"
	"github.com/cloudfoundry/volumedriver/oshelper"
	"github.com/orange-cloudfoundry/s3-volume-driver"
	"os"
	"path/filepath"

	cf_http "code.cloudfoundry.org/cfhttp"
	cf_debug_server "code.cloudfoundry.org/debugserver"
	"code.cloudfoundry.org/dockerdriver"
	"code.cloudfoundry.org/dockerdriver/driverhttp"
	"code.cloudfoundry.org/dockerdriver/invoker"
	"code.cloudfoundry.org/goshims/bufioshim"
	"code.cloudfoundry.org/goshims/filepathshim"
	"code.cloudfoundry.org/goshims/ioutilshim"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagerflags"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/http_server"
	"github.com/tedsuo/ifrit/sigmon"
)

var atAddress = flag.String(
	"listenAddr",
	"127.0.0.1:9750",
	"host:port to serve volume management functions",
)

var driversPath = flag.String(
	"driversPath",
	"",
	"Path to directory where drivers are installed",
)

var transport = flag.String(
	"transport",
	"tcp",
	"Transport protocol to transmit HTTP over",
)

var mountDir = flag.String(
	"mountDir",
	"/tmp/volumes",
	"Path to directory where fake volumes are created",
)

var requireSSL = flag.Bool(
	"requireSSL",
	false,
	"whether the fake driver should require ssl-secured communication",
)

var caFile = flag.String(
	"caFile",
	"",
	"the certificate authority public key file to use with ssl authentication",
)

var certFile = flag.String(
	"certFile",
	"",
	"the public key file to use with ssl authentication",
)

var keyFile = flag.String(
	"keyFile",
	"",
	"the private key file to use with ssl authentication",
)
var clientCertFile = flag.String(
	"clientCertFile",
	"",
	"the public key file to use with client ssl authentication",
)

var clientKeyFile = flag.String(
	"clientKeyFile",
	"",
	"the private key file to use with client ssl authentication",
)

var insecureSkipVerify = flag.Bool(
	"insecureSkipVerify",
	false,
	"whether SSL communication should skip verification of server IP addresses in the certificate",
)

var uniqueVolumeIds = flag.Bool(
	"uniqueVolumeIds",
	false,
	"whether the s3 driver should opt-in to unique volumes",
)

func main() {
	parseCommandLine()

	var localDriverServer ifrit.Runner

	logger, logTap := newLogger()
	logger.Info("start")
	defer logger.Info("end")

	client := s3driver.NewS3Driver(
		logger,
		&osshim.OsShim{},
		&filepathshim.FilepathShim{},
		&ioutilshim.IoutilShim{},
		&timeshim.TimeShim{},
		mountchecker.NewChecker(&bufioshim.BufioShim{}, &osshim.OsShim{}),
		*mountDir,
		oshelper.NewOsHelper(),
		invoker.NewRealInvoker(),
	)

	if *transport == "tcp" {
		localDriverServer = createS3DriverServer(logger, client, *atAddress, *driversPath, false, false)
	} else if *transport == "tcp-json" {
		localDriverServer = createS3DriverServer(logger, client, *atAddress, *driversPath, true, *uniqueVolumeIds)
	} else {
		localDriverServer = createS3DriverUnixServer(logger, client, *atAddress)
	}

	servers := grouper.Members{
		{Name: "localdriver-server", Runner: localDriverServer},
	}

	if dbgAddr := cf_debug_server.DebugAddress(flag.CommandLine); dbgAddr != "" {
		servers = append(grouper.Members{
			{Name: "debug-server", Runner: cf_debug_server.Runner(dbgAddr, logTap)},
		}, servers...)
	}

	process := ifrit.Invoke(processRunnerFor(servers))
	logger.Info("started")

	untilTerminated(logger, process)
}

func exitOnFailure(logger lager.Logger, err error) {
	if err != nil {
		logger.Error("fatal-err..aborting", err)
		panic(err.Error())
	}
}

func untilTerminated(logger lager.Logger, process ifrit.Process) {
	err := <-process.Wait()
	exitOnFailure(logger, err)
}

func processRunnerFor(servers grouper.Members) ifrit.Runner {
	return sigmon.New(grouper.NewOrdered(os.Interrupt, servers))
}

func createS3DriverServer(logger lager.Logger, client dockerdriver.Driver, atAddress, driversPath string, jsonSpec bool, uniqueVolumeIds bool) ifrit.Runner {
	advertisedUrl := "http://" + atAddress
	logger.Info("writing-spec-file", lager.Data{"location": driversPath, "name": "s3driver", "address": advertisedUrl, "unique-volume-ids": uniqueVolumeIds})
	if jsonSpec {
		driverJsonSpec := dockerdriver.DriverSpec{Name: "s3driver", Address: advertisedUrl, UniqueVolumeIds: uniqueVolumeIds}

		if *requireSSL {
			absCaFile, err := filepath.Abs(*caFile)
			exitOnFailure(logger, err)
			absClientCertFile, err := filepath.Abs(*clientCertFile)
			exitOnFailure(logger, err)
			absClientKeyFile, err := filepath.Abs(*clientKeyFile)
			exitOnFailure(logger, err)
			driverJsonSpec.TLSConfig = &dockerdriver.TLSConfig{InsecureSkipVerify: *insecureSkipVerify, CAFile: absCaFile, CertFile: absClientCertFile, KeyFile: absClientKeyFile}
			driverJsonSpec.Address = "https://" + atAddress
		}

		jsonBytes, err := json.Marshal(driverJsonSpec)

		exitOnFailure(logger, err)
		err = dockerdriver.WriteDriverSpec(logger, driversPath, "s3driver", "json", jsonBytes)
		exitOnFailure(logger, err)
	} else {
		err := dockerdriver.WriteDriverSpec(logger, driversPath, "s3driver", "spec", []byte(advertisedUrl))
		exitOnFailure(logger, err)
	}

	handler, err := driverhttp.NewHandler(logger, client)
	exitOnFailure(logger, err)

	var server ifrit.Runner
	if *requireSSL {
		tlsConfig, err := cf_http.NewTLSConfig(*certFile, *keyFile, *caFile)
		if err != nil {
			logger.Fatal("tls-configuration-failed", err)
		}
		server = http_server.NewTLSServer(atAddress, handler, tlsConfig)
	} else {
		server = http_server.New(atAddress, handler)
	}

	return server
}

func createS3DriverUnixServer(logger lager.Logger, client dockerdriver.Driver, atAddress string) ifrit.Runner {
	handler, err := driverhttp.NewHandler(logger, client)
	exitOnFailure(logger, err)
	return http_server.NewUnixServer(atAddress, handler)
}

func newLogger() (lager.Logger, *lager.ReconfigurableSink) {
	lagerConfig := lagerflags.ConfigFromFlags()
	lagerConfig.RedactSecrets = true

	return lagerflags.NewFromConfig("s3-driver-server", lagerConfig)
}

func parseCommandLine() {
	lagerflags.AddFlags(flag.CommandLine)
	cf_debug_server.AddFlags(flag.CommandLine)
	flag.Parse()
}
