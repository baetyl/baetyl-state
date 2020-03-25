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
	"github.com/valyala/fasthttp"
	"google.golang.org/grpc"
)

//Config config of state
type Config struct {
	Database database.Conf     `yaml:"database" json:"database" default:"{\"driver\":\"boltdb\",\"source\":\"var/lib/baetyl/db/kv.db\"}"`
	Grpc     link.ServerConfig `yaml:"grpc" json:"grpc" default:"{\"address\":\"tcp://:80\"}"`
	Http     HttpServerConfig  `yaml:"http" json:"http"`
}

// Server server to handle message
type Server struct {
	db  database.DB
	svr *grpc.Server
	log *log.Logger
}

// HttpServerConfig http server config
type HttpServerConfig struct {
	Address           string `yaml:"address" json:"address" default:":80"`
	utils.Certificate `yaml:",inline" json:",inline"`
}

// Authenticator authenticator to authenticate tokens
type Authenticator struct{}

//NewServer new server
func NewServer(cfg Config) (*Server, error) {
	logger := log.With()
	uri, err := utils.ParseURL(cfg.Grpc.Address)
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

	s, err := link.NewServer(cfg.Grpc, new(Authenticator))
	if err != nil {
		return nil, err
	}
	kv.RegisterKVServiceServer(s, NewKVService(db, logger))
	listener, err := net.Listen(uri.Scheme, uri.Host)
	if err != nil {
		return nil, err
	}
	go func() {
		logger.Info(fmt.Sprintf("grpc server is listening at %s.", cfg.Grpc.Address))
		if err := s.Serve(listener); err != nil {
			logger.Error("grpc server shutdown.", log.Error(err))
		}
	}()

	handler := NewKVHandler(db, logger)
	go func() {
		logger.Info("http server is running.", log.Any("address", cfg.Http.Address))
		if cfg.Http.Cert != "" || cfg.Http.Key != "" {
			if err := fasthttp.ListenAndServeTLS(cfg.Http.Address,
				cfg.Http.Cert, cfg.Http.Key, handler.initRouter()); err != nil {
				logger.Error("server shutdown.", log.Error(err))
			}
		} else {
			if err := fasthttp.ListenAndServe(cfg.Http.Address,
				handler.initRouter()); err != nil {
				logger.Error("http server shutdown.", log.Error(err))
			}
		}
	}()

	return &Server{
		svr: s,
		db:  db,
		log: logger,
	}, nil
}

// Close Close
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

// Authenticate authenticate
func (a Authenticator) Authenticate(ctx context.Context) error {
	//todo: how to store token beforehead
	return nil
}
