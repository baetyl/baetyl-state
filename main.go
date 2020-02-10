package main

import (
	"flag"
	// "net/http"
	// _ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/utils"
	_ "github.com/mattn/go-sqlite3"
)

var (
	h bool
	c string
)

func init() {
	flag.BoolVar(&h, "h", false, "this help")
	flag.StringVar(&c, "c", "etc/baetyl/service.yml", "set configuration file")
}

func main() {
	utils.Version()
	flag.Parse()
	if h {
		flag.Usage()
		return
	}

	l := log.With()
	defer l.Sync()

	var cfg Config
	if utils.FileExists(c) {
		utils.LoadYAML(c, &cfg)
	} else {
		utils.SetDefaults(&cfg)
	}
	s, err := NewServer(cfg)
	if err != nil {
		l.Fatal("failed to create broker", log.Error(err))
	}
	defer s.Close()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	signal.Ignore(syscall.SIGPIPE)
	<-sig
}
