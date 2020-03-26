package main

import (
	"github.com/baetyl/baetyl-go/context"
)

func main() {
	context.Run(func(ctx context.Context) error {
		var cfg Config
		err := ctx.LoadCustomConfig(&cfg)
		if err != nil {
			return err
		}
		s, err := NewServer(cfg)
		if err != nil {
			s.Close()
			return err
		}
		ctx.Wait()
		return nil
	})
}
