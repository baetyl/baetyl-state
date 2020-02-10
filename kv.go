package main

import (
	"fmt"
	"github.com/baetyl/baetyl-go/kv"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/baetyl/baetyl-state/database"
	"google.golang.org/grpc"
	"net"
)

//Config config of state
type Config struct {
	Database database.Conf `yaml:"database" json:"database" default:"{\"driver\":\"sqlite3\",\"path\":\"var/lib/baetyl/db/kv.db\"}"`
	Server   link.ServerConfig
}

// Server server to handle message
type Server struct {
	//conf link.ServerConfig
	svr *grpc.Server
}

func NewServer(cfg Config) (*Server, error) {
	logger := log.With(log.Any("main", "state"))
	uri, err := utils.ParseURL(cfg.Server.Address)
	if err != nil {
		return nil, err
	}
	d, err := database.New(database.Conf{Driver: cfg.Database.Driver, Source: cfg.Database.Source})
	if err != nil {
		d.Close()
		return nil, err
	}
	s, err := link.NewServer(cfg.Server, nil)
	if err != nil {
		return nil, err
	}
	kv.RegisterKVServiceServer(s, NewKVService(d))
	listener, err := net.Listen(uri.Scheme, uri.Host)
	if err != nil {
		return nil, err
	}
	logger.Info(fmt.Sprintf("server is listening at: %s", cfg.Server.Address))
	go func() {
		if err := s.Serve(listener); err != nil {
			logger.Error("api server shutdown.", log.Error(err))
		}
	}()
	return &Server{svr: s}, nil
}

func (s *Server) Close() {
	if s.svr != nil {
		s.svr.GracefulStop()
	}
	//log
}
