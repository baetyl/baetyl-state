package database

import (
	"errors"
	"io"

	"github.com/baetyl/baetyl-go/kv"
)

// Factories of database
var Factories = map[string]func(conf Conf) (DB, error){}

// DB the backend database
type DB interface {
	Conf() Conf

	Set(kv *kv.KV) error
	Get(key string) (*kv.KV, error)
	Del(key string) error
	List(prefix string) (*kv.KVs, error)

	io.Closer
}

// Conf the configuration of database
type Conf struct {
	Driver string
	Source string
}

// New KV database by given name
func New(conf Conf) (DB, error) {
	if f, ok := Factories[conf.Driver]; ok {
		return f(conf)
	}
	return nil, errors.New("no such kind database")
}
