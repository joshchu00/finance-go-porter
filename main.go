package main

import (
	"fmt"
	"net"
	"time"

	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/config"
	"github.com/joshchu00/finance-go-common/logger"
	"github.com/joshchu00/finance-go-porter/server"
	pb "github.com/joshchu00/finance-protobuf/porter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func init() {

	// config
	config.Init()

	// logger
	logger.Init(config.LogDirectory(), "porter")

	// log config
	logger.Info(fmt.Sprintf("%s: %s", "EnvironmentName", config.EnvironmentName()))
	logger.Info(fmt.Sprintf("%s: %s", "LogDirectory", config.LogDirectory()))
	logger.Info(fmt.Sprintf("%s: %s", "CassandraHosts", config.CassandraHosts()))
	logger.Info(fmt.Sprintf("%s: %s", "CassandraKeyspace", config.CassandraKeyspace()))
	logger.Info(fmt.Sprintf("%s: %s", "PorterV1Port", config.PorterV1Port()))
}

var environmentName string

func process() {

	if environmentName == config.EnvironmentNameProd {
		defer func() {
			if err := recover(); err != nil {
				logger.Panic(fmt.Sprintf("recover %v", err))
			}
		}()
	}

	var err error

	// cassandra client
	var cassandraClient *cassandra.Client
	cassandraClient, err = cassandra.NewClient(config.CassandraHosts(), config.CassandraKeyspace())
	if err != nil {
		logger.Panic(fmt.Sprintf("cassandra.NewClient %v", err))
	}
	defer cassandraClient.Close()

	// starting porter v1 server
	var listen net.Listener
	listen, err = net.Listen("tcp", fmt.Sprintf(":%s", config.PorterV1Port()))
	if err != nil {
		logger.Panic(fmt.Sprintf("net.Listen %v", err))
	}
	porterServer := grpc.NewServer()
	pb.RegisterPorterV1Server(porterServer, &server.PorterV1Server{CassandraClient: cassandraClient})
	// Register reflection service on gRPC server.
	reflection.Register(porterServer)
	if err = porterServer.Serve(listen); err != nil {
		logger.Panic(fmt.Sprintf("porterServer.Serve %v", err))
	}
}

func main() {

	logger.Info("Starting porter...")

	// environment name
	switch environmentName = config.EnvironmentName(); environmentName {
	case config.EnvironmentNameDev, config.EnvironmentNameTest, config.EnvironmentNameStg, config.EnvironmentNameProd:
	default:
		logger.Panic("Unknown environment name")
	}

	for {

		process()

		time.Sleep(3 * time.Second)

		if environmentName != config.EnvironmentNameProd {
			break
		}
	}
}
