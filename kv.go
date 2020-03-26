package main

import (
	"fmt"
	"os"
	"path"

	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/baetyl/baetyl-state/database"
	"github.com/valyala/fasthttp"
)

//Config config of state
type Config struct {
	Database database.Conf `yaml:"database" json:"database" default:"{\"driver\":\"boltdb\",\"source\":\"var/lib/baetyl/state.db\"}"`
	Server   ServerConfig  `yaml:"server" json:"server"`
}

// Server server to handle message
type Server struct {
	db  database.DB
	log *log.Logger
}

// ServerConfig http server config
type ServerConfig struct {
	Address           string `yaml:"address" json:"address" default:":80"`
	utils.Certificate `yaml:",inline" json:",inline"`
}

// NewServer new server
func NewServer(cfg Config) (*Server, error) {
	server := &Server{
		log: log.With(log.Any("main", "kv")),
	}

	err := os.MkdirAll(path.Dir(cfg.Database.Source), 0755)
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
	server.db = db
	server.log.Info("db inited", log.Any("driver", dbConf.Driver), log.Any("source", dbConf.Source))

	handler := NewKVHandler(db, log.With(log.Any("main", "handler")))
	go func() {
		server.log.Info("http server is running.", log.Any("address", cfg.Server.Address))
		if cfg.Server.Cert != "" || cfg.Server.Key != "" {
			if err := fasthttp.ListenAndServeTLS(cfg.Server.Address,
				cfg.Server.Cert, cfg.Server.Key, handler.initRouter()); err != nil {
				server.log.Error("server shutdown.", log.Error(err))
			}
		} else {
			if err := fasthttp.ListenAndServe(cfg.Server.Address,
				handler.initRouter()); err != nil {
				server.log.Error("http server shutdown.", log.Error(err))
			}
		}
	}()

	return server, nil
}

// Close Close
func (s *Server) Close() {
	if s.db != nil {
		s.db.Close()
		s.log.Info("db has closed")
	}
}
