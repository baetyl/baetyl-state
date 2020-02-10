package main

import (
	baetyl "github.com/baetyl/baetyl-go/context"
	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/utils"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	utils.Version()
	baetyl.Run(func(ctx baetyl.Context) error {
		var cfg Config
		err := ctx.LoadConfig(&cfg)
		if err != nil {
			return err
		}
		s, err := NewServer(cfg)
		if err != nil {
			ctx.Log().Fatal("failed to start baetyl-state", log.Error(err))
		}
		defer s.Close()
		ctx.Wait()
		return nil
	})
}
