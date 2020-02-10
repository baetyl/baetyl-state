package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"

	"github.com/baetyl/baetyl-go/kv"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/baetyl/baetyl-state/database"
	"google.golang.org/grpc"
)

//Config config of state
type Config struct {
	Database database.Conf     `yaml:"database" json:"database" default:"{\"driver\":\"sqlite3\",\"source\":\"var/lib/baetyl/db/kv.db\"}"`
	Server   link.ServerConfig `yaml:"server" json:"server" default:"{\"address\":\"tcp://127.0.0.1:50040\"}"`
}

// Server server to handle message
type Server struct {
	svr *grpc.Server
	db  database.DB
	log *log.Logger
}

// Authenticator authenticator to authenticate tokens
type Authenticator struct{}

//NewServer new grpc server
func NewServer(cfg Config) (*Server, error) {
	logger := log.With()
	uri, err := utils.ParseURL(cfg.Server.Address)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(path.Dir(cfg.Database.Source), 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to make db directory: %s", err.Error())
	}
	dbConf := database.Conf{
		Driver: cfg.Database.Driver,
		Source: cfg.Database.Source,
	}
	db, err := database.New(dbConf)
	if err != nil {
		return nil, err
	}
	logger.Info("db inited", log.Any("driver", dbConf.Driver), log.Any("source", dbConf.Source))
	s, err := link.NewServer(cfg.Server, new(Authenticator))
	if err != nil {
		return nil, err
	}
	kv.RegisterKVServiceServer(s, NewKVService(db, logger))
	listener, err := net.Listen(uri.Scheme, uri.Host)
	if err != nil {
		return nil, err
	}
	go func() {
		logger.Info(fmt.Sprintf("server is listening at %s.", cfg.Server.Address))
		if err := s.Serve(listener); err != nil {
			logger.Error("server shutdown.", log.Error(err))
		}
	}()
	return &Server{
		svr: s,
		db:  db,
		log: logger,
	}, nil
}

func (s *Server) Close() {
	defer s.log.Info("server has closed")
	if s.svr != nil {
		s.svr.Stop()
	}
	if s.db != nil {
		s.db.Close()
		s.log.Info("db has closed")
	}
}

func (a Authenticator) Authenticate(ctx context.Context) error {
	//todo: how to store token beforehead
	return nil
}
