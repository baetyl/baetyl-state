package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/baetyl/baetyl-go/kv"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/baetyl/baetyl-state/database"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestGrpcKV(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	conf := Config{}
	_, err = NewServer(conf)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "no such kind database")

	conf = Config{
		Database: database.Conf{
			Driver: "boltdb",
			Source: path.Join(dir, "kv1.db"),
		},
		Grpc: link.ServerConfig{
			Address: "tcp://127.0.0.1:10000000",
		},
	}
	_, err = NewServer(conf)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "listen tcp: address 10000000: invalid port")

	ctx := context.Background()
	confs := []struct {
		serverConf Config
		cliConf    mockClientConfig
	}{
		{
			serverConf: Config{
				Database: database.Conf{
					Driver: "sqlite3",
					Source: path.Join(dir, "kv2.db"),
				},
				Grpc: link.ServerConfig{
					Address: "tcp://127.0.0.1:50060",
					Certificate: utils.Certificate{
						CA:   "./example/var/lib/baetyl/testcert/ca.pem",
						Key:  "./example/var/lib/baetyl/testcert/server.key",
						Cert: "./example/var/lib/baetyl/testcert/server.pem",
						Name: "bd",
					},
					MaxMessageSize: 1024,
				},
			},
			cliConf: mockClientConfig{
				Address: "127.0.0.1:50060",
				Certificate: utils.Certificate{
					CA:   "./example/var/lib/baetyl/testcert/ca.pem",
					Key:  "./example/var/lib/baetyl/testcert/client.key",
					Cert: "./example/var/lib/baetyl/testcert/client.pem",
					Name: "bd",
				},
			},
		},
		{
			serverConf: Config{
				Database: database.Conf{
					Driver: "sqlite3",
					Source: path.Join(dir, "kv3.db"),
				},
				Grpc: link.ServerConfig{
					Address:        "tcp://127.0.0.1:50060",
					MaxMessageSize: 1024,
				},
			},
			cliConf: mockClientConfig{
				Address: "127.0.0.1:50060",
			},
		},
	}
	for _, conf := range confs {
		server, err := NewServer(conf.serverConf)
		assert.NoError(t, err)
		assert.NotEmpty(t, server)

		cli, err := newmockClient(conf.cliConf)
		assert.NoError(t, err)
		assert.NotEmpty(t, cli)

		_, err = cli.KV.Get(ctx, &kv.KV{Key: "aa"})
		assert.NoError(t, err)

		_, err = cli.KV.Set(ctx, &kv.KV{Key: "aa"})
		assert.NoError(t, err)

		_, err = cli.KV.Set(ctx, &kv.KV{Key: "aa", Value: []byte("")})
		assert.NoError(t, err)

		_, err = cli.KV.Set(ctx, &kv.KV{Key: "aa", Value: []byte("aadata")})
		assert.NoError(t, err)

		resp, err := cli.KV.Get(ctx, &kv.KV{Key: "aa"})
		assert.NoError(t, err)
		assert.Equal(t, resp.Key, "aa")
		assert.Equal(t, resp.Value, []byte("aadata"))

		_, err = cli.KV.Del(ctx, &kv.KV{Key: "aa"})
		assert.NoError(t, err)

		_, err = cli.KV.Del(ctx, &kv.KV{Key: ""})
		assert.NoError(t, err)

		resp, err = cli.KV.Get(ctx, &kv.KV{Key: "aa"})
		assert.NoError(t, err)
		assert.Equal(t, resp.Key, "aa")
		assert.Empty(t, resp.Value)

		_, err = cli.KV.Set(ctx, &kv.KV{Key: "/a", Value: []byte("/root/a")})
		assert.NoError(t, err)

		_, err = cli.KV.Set(ctx, &kv.KV{Key: "/b", Value: []byte("/root/b")})
		assert.NoError(t, err)

		_, err = cli.KV.Set(ctx, &kv.KV{Key: "/c", Value: []byte("/root/c")})
		assert.NoError(t, err)

		respa, err := cli.KV.List(ctx, &kv.KV{Key: "/"})
		assert.NoError(t, err)
		assert.Len(t, respa.Kvs, 3)

		server.Close()
		cli.Close()
	}
}

func TestHttpKV(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	_conf := Config{}
	_, err = NewServer(_conf)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "no such kind database")

	//ctx := context.Background()
	conf := struct {
		serverConf Config
		cliConf    mockClientConfig
	}{
		serverConf: Config{
			Database: database.Conf{
				Driver: "boltdb",
				Source: path.Join(dir, "kv3.db"),
			},
			Grpc: link.ServerConfig{
				Address: "tcp://127.0.0.1:50060",
			},
			Http: HttpServerConfig{
				Address: "127.0.0.1:50030",
			},
		},
		cliConf: mockClientConfig{
			Address: "http://127.0.0.1:50030",
		},
	}
	server, err := NewServer(conf.serverConf)
	assert.NoError(t, err)
	assert.NotEmpty(t, server)
	time.Sleep(time.Second)

	kv1 := kv.KV{
		Key:   "key1",
		Value: []byte("value1"),
	}
	data, err := json.Marshal(kv1)
	assert.NoError(t, err)

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	url := conf.cliConf.Address
	req.SetRequestURI(url)
	req.Header.SetMethod("POST")
	req.SetBody(data)

	client := &fasthttp.Client{}
	err = client.Do(req, resp)
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode(), 200)

	req2 := fasthttp.AcquireRequest()
	resp2 := fasthttp.AcquireResponse()
	url2 := fmt.Sprintf("%s/%s", conf.cliConf.Address, "key1")
	req2.SetRequestURI(url2)
	req2.Header.SetMethod("GET")
	err = client.Do(req2, resp2)
	assert.NoError(t, err)
	assert.Equal(t, resp2.StatusCode(), 200)

	_kv2 := new(kv.KV)
	err = json.Unmarshal(resp2.Body(), _kv2)
	assert.NoError(t, err)
	assert.Equal(t, _kv2.Key, "key1")
	assert.Equal(t, _kv2.Value, []byte("value1"))

	req3 := fasthttp.AcquireRequest()
	resp3 := fasthttp.AcquireResponse()
	url3 := fmt.Sprintf("%s/%s", conf.cliConf.Address, "key1")
	req3.SetRequestURI(url3)
	req3.Header.SetMethod("DELETE")
	err = client.Do(req3, resp3)
	assert.NoError(t, err)
	assert.Equal(t, resp3.StatusCode(), 200)

	req4 := fasthttp.AcquireRequest()
	resp4 := fasthttp.AcquireResponse()
	url4 := fmt.Sprintf("%s/%s", conf.cliConf.Address, "key1")
	req4.SetRequestURI(url4)
	req4.Header.SetMethod("GET")
	err = client.Do(req4, resp4)
	assert.NoError(t, err)
	assert.Equal(t, resp4.StatusCode(), 200)

	_kv4 := new(kv.KV)
	err = json.Unmarshal(resp4.Body(), _kv4)
	assert.NoError(t, err)
	assert.Equal(t, _kv4.Key, "key1")

	kv2 := kv.KV{
		Key:   "key2",
		Value: []byte("value2"),
	}
	kv3 := kv.KV{
		Key:   "xey3",
		Value: []byte("value3"),
	}
	data2, err := json.Marshal(kv2)
	assert.NoError(t, err)
	data3, err := json.Marshal(kv3)
	assert.NoError(t, err)

	req5 := fasthttp.AcquireRequest()
	resp5 := fasthttp.AcquireResponse()
	url5 := conf.cliConf.Address
	req5.SetRequestURI(url5)
	req5.Header.SetMethod("POST")
	req5.SetBody(data2)
	err = client.Do(req5, resp5)
	assert.NoError(t, err)
	assert.Equal(t, resp5.StatusCode(), 200)

	req6 := fasthttp.AcquireRequest()
	resp6 := fasthttp.AcquireResponse()
	url6 := conf.cliConf.Address
	req6.SetRequestURI(url6)
	req6.Header.SetMethod("POST")
	req6.SetBody(data3)
	err = client.Do(req6, resp6)
	assert.NoError(t, err)
	assert.Equal(t, resp6.StatusCode(), 200)

	req8 := fasthttp.AcquireRequest()
	resp8 := fasthttp.AcquireResponse()
	url8 := conf.cliConf.Address
	req8.SetRequestURI(url8)
	req8.URI().SetQueryString("prefix=")
	req8.Header.SetMethod("GET")
	err = client.Do(req8, resp8)
	assert.NoError(t, err)
	assert.Equal(t, resp8.StatusCode(), 200)
	_kvs8 := new(kv.KVs)
	err = json.Unmarshal(resp8.Body(), _kvs8)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(_kvs8.Kvs))

	req9 := fasthttp.AcquireRequest()
	resp9 := fasthttp.AcquireResponse()
	url9 := conf.cliConf.Address
	req9.SetRequestURI(url9)
	req9.URI().SetQueryString("prefix=xey")
	req9.Header.SetMethod("GET")
	err = client.Do(req9, resp9)
	assert.NoError(t, err)
	assert.Equal(t, resp9.StatusCode(), 200)
	_kvs9 := new(kv.KVs)
	err = json.Unmarshal(resp9.Body(), _kvs9)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(_kvs9.Kvs))
}

func TestHttpKVFail(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)
	registerMockDB()

	conf := struct {
		serverConf Config
		cliConf    mockClientConfig
	}{
		serverConf: Config{
			Database: database.Conf{
				Driver: "errdb",
				Source: path.Join(dir, "kv3.db"),
			},
			Grpc: link.ServerConfig{
				Address: "tcp://127.0.0.1:50070",
			},
			Http: HttpServerConfig{
				Address: "127.0.0.1:50040",
			},
		},
		cliConf: mockClientConfig{
			Address: "http://127.0.0.1:50040",
		},
	}
	server, err := NewServer(conf.serverConf)
	assert.NoError(t, err)
	assert.NotEmpty(t, server)
	time.Sleep(time.Second)

	kv1 := kv.KV{
		Key:   "key1",
		Value: []byte("value1"),
	}
	data, err := json.Marshal(kv1)
	assert.NoError(t, err)

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	url := conf.cliConf.Address
	req.SetRequestURI(url)
	req.Header.SetMethod("POST")
	req.SetBody(data)
	client := &fasthttp.Client{}
	err = client.Do(req, resp)
	assert.NoError(t, err)
	assert.Equal(t, resp.StatusCode(), 500)

	req2 := fasthttp.AcquireRequest()
	resp2 := fasthttp.AcquireResponse()
	url2 := fmt.Sprintf("%s/%s", conf.cliConf.Address, "key1")
	req2.SetRequestURI(url2)
	req2.Header.SetMethod("GET")
	err = client.Do(req2, resp2)
	assert.NoError(t, err)
	assert.Equal(t, resp2.StatusCode(), 500)

	req3 := fasthttp.AcquireRequest()
	resp3 := fasthttp.AcquireResponse()
	url3 := fmt.Sprintf("%s/%s", conf.cliConf.Address, "key1")
	req3.SetRequestURI(url3)
	req3.Header.SetMethod("DELETE")
	err = client.Do(req3, resp3)
	assert.NoError(t, err)
	assert.Equal(t, resp3.StatusCode(), 500)

	req4 := fasthttp.AcquireRequest()
	resp4 := fasthttp.AcquireResponse()
	url4 := conf.cliConf.Address
	req4.SetRequestURI(url4)
	req4.URI().SetQueryString("prefix=")
	req4.Header.SetMethod("GET")
	err = client.Do(req4, resp4)
	assert.NoError(t, err)
	assert.Equal(t, resp4.StatusCode(), 500)
}

type mockDB struct{}

func registerMockDB() {
	database.Factories["errdb"] = newMockDB
}

// New creates a new sql database
func newMockDB(conf database.Conf) (database.DB, error) {
	return new(mockDB), nil
}

func (d *mockDB) Conf() database.Conf {
	return database.Conf{}
}

// Set put key and value into SQL DB
func (d *mockDB) Set(kv *kv.KV) error {
	return errors.New("custom error")
}

// Get gets value by key from SQL DB
func (d *mockDB) Get(key string) (*kv.KV, error) {
	return nil, errors.New("custom error")
}

// Del deletes key and value from SQL DB
func (d *mockDB) Del(key string) error {
	return errors.New("custom error")
}

// List list kvs with the prefix
func (d *mockDB) List(prefix string) (*kv.KVs, error) {
	return nil, errors.New("custom error")
}

func (d *mockDB) Close() error {
	return nil
}

// ClientConfig api client config
type mockClientConfig struct {
	Address           string `yaml:"address" json:"address"`
	utils.Certificate `yaml:",inline" json:",inline"`
}
type mockClient struct {
	conn *grpc.ClientConn
	KV   kv.KVServiceClient
}

func newmockClient(conf mockClientConfig) (*mockClient, error) {
	ctx := context.Background()

	opts := []grpc.DialOption{
		grpc.WithBlock(),
	}

	if conf.Key != "" || conf.Cert != "" {
		tlsCfg, err := utils.NewTLSConfigClient(conf.Certificate)
		if err != nil {
			return nil, err
		}
		if !conf.InsecureSkipVerify {
			tlsCfg.ServerName = conf.Name
		}
		creds := credentials.NewTLS(tlsCfg)
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	conn, err := grpc.DialContext(ctx, conf.Address, opts...)
	if err != nil {
		return nil, err
	}
	kv := kv.NewKVServiceClient(conn)
	return &mockClient{
		conn: conn,
		KV:   kv,
	}, nil
}

// Close closes the client
func (c *mockClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
