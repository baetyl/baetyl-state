package main

import (
	"context"
	"fmt"
	"net"

	"github.com/baetyl/baetyl-go/kv"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/baetyl/baetyl-state/database"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

//Config config of state
type Config struct {
	Database database.Conf
	Server   link.ServerConfig
}

// Server server to handle message
type Server struct {
	svr *grpc.Server
	log *log.Logger
}

// Authenticator authenticator to authenticate tokens
type Authenticator struct{}

func NewServer(cfg Config) (*Server, error) {
	logger := log.With(log.Any("main", "baetyl-state"))
	uri, err := utils.ParseURL(cfg.Server.Address)
	if err != nil {
		return nil, err
	}
	dbConf := database.Conf{
		Driver: cfg.Database.Driver,
		Source: cfg.Database.Source,
	}
	d, err := database.New(dbConf)
	if err != nil {
		d.Close()
		return nil, err
	}
	s, err := link.NewServer(cfg.Server, new(Authenticator))
	if err != nil {
		return nil, err
	}
	kv.RegisterKVServiceServer(s, NewKVService(d, logger))
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
		log: logger,
	}, nil
}

func (s *Server) Close() {
	defer s.log.Info("server has closed")
	if s.svr != nil {
		s.svr.Stop()
	}
}

func (a Authenticator) Authenticate(ctx context.Context) error {
	//todo: how to store token beforehead
	metadata.FromIncomingContext(ctx)
	//md, ok := metadata.FromIncomingContext(ctx)
	//if !ok {
	//	return link.ErrUnauthenticated
	//}

	//var u, p string
	//if val, ok := md[link.KeyUsername]; ok {
	//	u = val[0]
	//}
	//if val, ok := md[link.KeyPassword]; ok {
	//	p = val[0]
	//}
	//if ok := a.m.Auth(u, p); !ok {
	//	return link.ErrUnauthenticated
	//}
	return nil
}
