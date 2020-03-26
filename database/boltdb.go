package database

import (
	"bytes"
	"time"

	bolt "go.etcd.io/bbolt"
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
func (d *boltDb) Set(kv *KV) error {
	return d.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(d.bucket)
		if err != nil {
			return err
		}
		err = b.Put([]byte(kv.Key), kv.Value)
		return err
	})
}

// Get gets value by key from BoltDB
func (d *boltDb) Get(key string) (kv *KV, err error) {
	err = d.View(func(tx *bolt.Tx) error {
		kv = &KV{Key: key}
		b := tx.Bucket(d.bucket)
		if b == nil {
			return nil
		}
		iv := b.Get([]byte(key))
		if len(iv) == 0 {
			return nil
		}
		kv.Value = make([]byte, len(iv))
		copy(kv.Value, iv)
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
func (d *boltDb) List(prefix string) (kvs []KV, err error) {
	err = d.View(func(tx *bolt.Tx) error {
		//var kvs []KV
		b := tx.Bucket(d.bucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()

		prefix := []byte(prefix)
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var kv KV
			kv.Key = string(k)
			kv.Value = make([]byte, len(v))
			copy(kv.Value, v)
			kvs = append(kvs, kv)
		}
		return nil
	})
	return
}
