package main

import (
	"fmt"
	"os"
	"path"

	"github.com/baetyl/baetyl-go/http"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-state/database"
)

//Config config of state
type Config struct {
	Database database.Conf     `yaml:"database" json:"database" default:"{\"driver\":\"boltdb\",\"source\":\"var/lib/baetyl/state.db\"}"`
	Server   http.ServerConfig `yaml:"server" json:"server"`
}

// Server server to handle message
type Server struct {
	svr *http.Server
	db  database.DB
	log *log.Logger
}

// NewServer new server
func NewServer(cfg Config) (*Server, error) {
	server := &Server{
		log: log.L(),
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
	server.svr = http.NewServer(cfg.Server, handler.initRouter())
	server.svr.Start()
	return server, nil
}

// Close Close
func (s *Server) Close() {
	if s.svr != nil {
		s.svr.Close()
		s.log.Info("server has closed")
	}
	if s.db != nil {
		s.db.Close()
		s.log.Info("db has closed")
	}
}
