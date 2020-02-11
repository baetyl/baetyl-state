package main

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/baetyl/baetyl-go/kv"
	"github.com/baetyl/baetyl-go/link"
	"github.com/baetyl/baetyl-go/utils"
	"github.com/baetyl/baetyl-state/database"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestKV(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	conf := Config{}
	_, err = NewServer(conf)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "no such kind database")

	conf = Config{
		Database: database.Conf{
			Driver: "sqlite3",
			Source: path.Join(dir, "kv1.db"),
		},
		Server: link.ServerConfig{
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
				Server: link.ServerConfig{
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
				Server: link.ServerConfig{
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
