package database

import (
	"bytes"
	"time"

	"github.com/baetyl/baetyl-go/kv"
	"github.com/boltdb/bolt"
)

func init() {
	Factories["boltdb"] = newBoltDB
}

type boltDb struct {
	*bolt.DB
	bucket []byte
	conf   Conf
}

func newBoltDB(conf Conf) (DB, error) {
	db, err := bolt.Open(conf.Source, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}

	return &boltDb{
		DB:     db,
		bucket: []byte(".self"),
		conf:   conf,
	}, nil
}

// Conf returns the configuration
func (d *boltDb) Conf() Conf {
	return d.conf
}

// Set put key and value into BoltDB
func (d *boltDb) Set(kv *kv.KV) error {
	return d.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(d.bucket)
		if err != nil {
			return err
		}
		return b.Put([]byte(kv.Key), kv.Value)
	})
}

// Get gets value by key from BoltDB
func (d *boltDb) Get(key string) (_kv *kv.KV, err error) {
	err = d.View(func(tx *bolt.Tx) error {
		_kv = &kv.KV{Key: key}
		b := tx.Bucket(d.bucket)
		if b == nil {
			return nil
		}
		iv := b.Get([]byte(key))
		if len(iv) == 0 {
			return nil
		}
		_kv.Value = make([]byte, len(iv))
		copy(_kv.Value, iv)
		return nil
	})
	return
}

// Del deletes key and value from BoltDB
func (d *boltDb) Del(key string) error {
	return d.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(d.bucket)
		if b == nil {
			return nil
		}
		return b.Delete([]byte(key))
	})
}

// List list kvs with the prefix from BoltDB
func (d *boltDb) List(prefix string) (kvs *kv.KVs, err error) {
	err = d.View(func(tx *bolt.Tx) error {
		kvs = new(kv.KVs)
		// Assume bucket exists and has keys
		b := tx.Bucket(d.bucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()

		prefix := []byte(prefix)
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			_kv := new(kv.KV)
			_kv.Key = string(k)
			_kv.Value = make([]byte, len(v))
			copy(_kv.Value, v)
			kvs.Kvs = append(kvs.Kvs, _kv)
		}
		return nil
	})
	return
}
