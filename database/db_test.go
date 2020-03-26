package database

import (
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestNewSqlite(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	_, err = New(Conf{Driver: "sqlite2", Source: path.Join(dir, "kv.db")})
	assert.Error(t, err)

	_, err = New(Conf{Driver: "sqlite3", Source: path.Join(dir, "kv.db")})
	assert.NoError(t, err)

	_, err = New(Conf{Driver: "sqlite3", Source: "var/lib/kv.db"})
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "unable to open database file: no such file or directory")
}

func TestSqliteConf(t *testing.T) {
	conf := Conf{Driver: "sqlite3", Source: path.Join("test", "kv.db")}
	db := sqldb{nil, conf}
	assert.Equal(t, db.Conf(), conf)
}

func TestNewBoltDB(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	_, err = New(Conf{Driver: "unknown", Source: path.Join(dir, "kv.db")})
	assert.Error(t, err)

	_, err = New(Conf{Driver: "boltdb", Source: path.Join(dir, "kv.db")})
	assert.NoError(t, err)

	_, err = New(Conf{Driver: "boltdb", Source: "var/lib/kv.db"})
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "open var/lib/kv.db: no such file or directory")
}

func TestBoltDbConf(t *testing.T) {
	conf := Conf{Driver: "boltdb", Source: path.Join("test", "kv.db")}
	db := sqldb{nil, conf}
	assert.Equal(t, db.Conf(), conf)
}

func TestDatabaseKV(t *testing.T) {
	dir, err := ioutil.TempDir("", t.Name())
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	confs := []Conf{
		{
			Driver: "sqlite3",
			Source: path.Join(dir, "kv1.db"),
		},
		{
			Driver: "boltdb",
			Source: path.Join(dir, "kv2.db"),
		},
	}

	for _, conf := range confs {
		db, err := New(conf)
		assert.NoError(t, err)
		assert.NotNil(t, db)
		assert.Equal(t, conf.Driver, db.Conf().Driver)
		defer db.Close()

		k1 := KV{
			Key:   "k1",
			Value: []byte("k1data"),
		}

		// Get: k1 does not exist
		v, err := db.Get(k1.Key)
		assert.NoError(t, err)
		assert.Equal(t, k1.Key, v.Key)
		assert.Empty(t, v.Value)

		// Put: k1 does not exist
		err = db.Set(&k1)
		assert.NoError(t, err)

		// Get: k1 exists
		v, err = db.Get(k1.Key)
		assert.NoError(t, err)
		assert.Equal(t, k1.Key, v.Key)
		assert.Equal(t, k1.Value, v.Value)

		// Put: k1 again
		err = db.Set(&k1)
		assert.NoError(t, err)

		// Put: key is empty
		err = db.Set(&KV{})
		assert.Error(t, err)

		// Put: value is empty
		err = db.Set(&KV{Key: "baetyl"})
		assert.NoError(t, err)

		// Del: del k1
		err = db.Del(k1.Key)
		assert.NoError(t, err)

		// Del: del k1
		err = db.Del("ss")
		assert.NoError(t, err)

		// list db
		vs, err := db.List("/")
		assert.NoError(t, err)
		assert.Len(t, vs, 0)

		kv1 := &KV{Key: "/k/1", Value: []byte("/k/1/data")}
		kv2 := &KV{Key: "/k/2", Value: []byte("/k/2/data")}
		kv3 := &KV{Key: "/s/3", Value: []byte("/d/3/data")}

		// put url-like key
		err = db.Set(kv1)
		assert.NoError(t, err)

		// put url-like key
		err = db.Set(kv2)
		assert.NoError(t, err)

		// put url-like key
		err = db.Set(kv3)
		assert.NoError(t, err)

		vs, err = db.List("")
		assert.NoError(t, err)
		assert.Len(t, vs, 4)

		//list db
		vs, err = db.List("/")
		assert.NoError(t, err)
		assert.Len(t, vs, 3)
		assert.Equal(t, vs[0].Key, kv1.Key)
		assert.Equal(t, vs[1].Key, kv2.Key)
		assert.Equal(t, vs[2].Key, kv3.Key)
		assert.Equal(t, vs[0].Value, kv1.Value)
		assert.Equal(t, vs[1].Value, kv2.Value)
		assert.Equal(t, vs[2].Value, kv3.Value)

		vs, err = db.List("/k")
		assert.NoError(t, err)
		assert.Len(t, vs, 2)
		assert.Equal(t, vs[0].Key, kv1.Key)
		assert.Equal(t, vs[1].Key, kv2.Key)
		assert.Equal(t, vs[0].Value, kv1.Value)
		assert.Equal(t, vs[0].Value, kv1.Value)

		vs, err = db.List("/k/")
		assert.NoError(t, err)
		assert.Len(t, vs, 2)
		assert.Equal(t, vs[0].Key, kv1.Key)
		assert.Equal(t, vs[1].Key, kv2.Key)
		assert.Equal(t, vs[0].Value, kv1.Value)
		assert.Equal(t, vs[0].Value, kv1.Value)

		vs, err = db.List("/kx/")
		assert.NoError(t, err)
		assert.Len(t, vs, 0)

		err = db.Del(kv1.Key)
		assert.NoError(t, err)

		// list db
		vs, err = db.List("/")
		assert.NoError(t, err)
		assert.Len(t, vs, 2)
		assert.Equal(t, vs[0].Key, kv2.Key)
		assert.Equal(t, vs[1].Key, kv3.Key)
		assert.Equal(t, vs[0].Value, kv2.Value)
		assert.Equal(t, vs[1].Value, kv3.Value)

		// delete k3
		err = db.Del(kv3.Key)
		assert.NoError(t, err)

		// list db
		vs, err = db.List("/kx")
		assert.NoError(t, err)
		assert.Len(t, vs, 0)

		// delete k2
		err = db.Del(kv2.Key)
		assert.NoError(t, err)

		// list db
		vs, err = db.List("/")
		assert.NoError(t, err)
		assert.Len(t, vs, 0)

		// test Chinese
		kvc1 := &KV{Key: "/陈/张", Value: []byte("/陈/张里")}
		kvc2 := &KV{Key: "/陈/王", Value: []byte("/陈/王里")}
		kvc3 := &KV{Key: "/李/王", Value: []byte("/李/王里")}

		// put url-like key
		err = db.Set(kvc1)
		assert.NoError(t, err)

		// put url-like key
		err = db.Set(kvc2)
		assert.NoError(t, err)

		// put url-like key
		err = db.Set(kvc3)
		assert.NoError(t, err)

		// list db
		vs, err = db.List("/陈")
		assert.NoError(t, err)
		assert.Len(t, vs, 2)
		assert.Equal(t, vs[0].Key, kvc1.Key)
		assert.Equal(t, vs[1].Key, kvc2.Key)
		assert.Equal(t, vs[0].Value, kvc1.Value)
		assert.Equal(t, vs[0].Value, kvc1.Value)

		vs, err = db.List("/陈/")
		assert.NoError(t, err)
		assert.Len(t, vs, 2)
		assert.Equal(t, vs[0].Key, kvc1.Key)
		assert.Equal(t, vs[1].Key, kvc2.Key)
		assert.Equal(t, vs[0].Value, kvc1.Value)
		assert.Equal(t, vs[0].Value, kvc1.Value)

		vs, err = db.List("/赵/")
		assert.NoError(t, err)
		assert.Len(t, vs, 0)

		vs, err = db.List("/李/")
		assert.NoError(t, err)
		assert.Len(t, vs, 1)
		assert.Equal(t, vs[0].Key, kvc3.Key)
		assert.Equal(t, vs[0].Value, kvc3.Value)

		err = db.Del(kvc1.Key)
		assert.NoError(t, err)

		vs, err = db.List("/陈")
		assert.NoError(t, err)
		assert.Len(t, vs, 1)
		assert.Equal(t, vs[0].Key, kvc2.Key)
		assert.Equal(t, vs[0].Value, kvc2.Value)

		err = db.Del(kvc3.Key)
		assert.NoError(t, err)

		vs, err = db.List("/李")
		assert.NoError(t, err)
		assert.Len(t, vs, 0)

		vs, err = db.List("/赵")
		assert.NoError(t, err)
		assert.Len(t, vs, 0)

		err = db.Del(kvc2.Key)
		assert.NoError(t, err)

		vs, err = db.List("/陈")
		assert.NoError(t, err)
		assert.Len(t, vs, 0)
	}
}

func BenchmarkDatabaseSQLite(b *testing.B) {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(b, err)
	defer os.RemoveAll(dir)

	db, err := New(Conf{Driver: "sqlite3", Source: path.Join(dir, "t.db")})
	assert.NoError(b, err)
	assert.NotNil(b, db)
	defer db.Close()

	// list db
	vs, err := db.List("/")
	assert.NoError(b, err)
	assert.Len(b, vs, 0)

	k1 := "/"
	b.ResetTimer()
	b.Run("put", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := k1 + strconv.Itoa(i)
			db.Set(&KV{Key: key, Value: []byte(key)})
		}
	})
	b.Run("get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := k1 + strconv.Itoa(i)
			db.Get(key)
		}
	})
	b.Run("del", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := k1 + strconv.Itoa(i)
			db.Del(key)
		}
	})
}

func BenchmarkDatabaseBoltDB(b *testing.B) {
	dir, err := ioutil.TempDir("", "")
	assert.NoError(b, err)
	defer os.RemoveAll(dir)

	db, err := New(Conf{Driver: "boltdb", Source: path.Join(dir, "boltdb.db")})
	assert.NoError(b, err)
	assert.NotNil(b, db)
	defer db.Close()

	// list db
	vs, err := db.List("/")
	assert.NoError(b, err)
	assert.Len(b, vs, 0)

	k1 := "/"
	b.ResetTimer()
	b.Run("put", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := k1 + strconv.Itoa(i)
			db.Set(&KV{Key: key, Value: []byte(key)})
		}
	})
	b.Run("get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := k1 + strconv.Itoa(i)
			db.Get(key)
		}
	})
	b.Run("del", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := k1 + strconv.Itoa(i)
			db.Del(key)
		}
	})
}
