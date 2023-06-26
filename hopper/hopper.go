package hopper

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"

	"go.etcd.io/bbolt"
)

const (
	defaultDBName = "default"
	ext           = "hopper"
)

type Map map[string]any

type Filter struct {
	EQ    map[string]any
	Limit int
	Sort  string
}

type Hopper struct {
	*Options
	db *bbolt.DB
}

type OptFunc func(opts *Options)

type Options struct {
	DBName string
}

func WithDBName(name string) OptFunc {
	return func(opts *Options) {
		opts.DBName = name
	}
}

func New(options ...OptFunc) (*Hopper, error) {
	opts := &Options{
		DBName: defaultDBName,
	}
	for _, fn := range options {
		fn(opts)
	}
	dbname := fmt.Sprintf("%s.%s", opts.DBName, ext)
	db, err := bbolt.Open(dbname, 0666, nil)
	if err != nil {
		return nil, err
	}
	return &Hopper{
		db:      db,
		Options: opts,
	}, nil
}

func (h *Hopper) DropDatabase(name string) error {
	dbname := fmt.Sprintf("%s.%s", name, ext)
	return os.Remove(dbname)
}

func (h *Hopper) CreateCollection(name string) (*bbolt.Bucket, error) {
	tx, err := h.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket, err := tx.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return nil, err
	}
	return bucket, err
}

func (h *Hopper) Insert(collName string, data Map) (uint64, error) {
	tx, err := h.db.Begin(true)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	collBucket, err := tx.CreateBucketIfNotExists([]byte(collName))
	if err != nil {
		return 0, err
	}
	id, err := collBucket.NextSequence()
	if err != nil {
		return 0, err
	}
	b, err := json.Marshal(data)
	if err != nil {
		return 0, err
	}
	if err := collBucket.Put(uint64Bytes(id), b); err != nil {
		return 0, err
	}
	return id, tx.Commit()
}

func (h *Hopper) Find(coll string, filter Filter) ([]Map, error) {
	tx, err := h.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte(coll))
	if bucket == nil {
		return nil, fmt.Errorf("collection (%s) not found", coll)
	}
	results := []Map{}
	bucket.ForEach(func(k, v []byte) error {
		data := Map{
			"id": uint64FromBytes(k),
		}
		if err := json.Unmarshal(v, &data); err != nil {
			return err
		}
		include := true
		if filter.EQ != nil {
			include = false
			for fk, fv := range filter.EQ {
				if value, ok := data[fk]; ok {
					if fv == value {
						include = true
					}
				}
			}
		}
		if include {
			results = append(results, data)
		}
		return nil
	})
	return results, tx.Commit()
}

func uint64Bytes(n uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, n)
	return b
}

func uint64FromBytes(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}
