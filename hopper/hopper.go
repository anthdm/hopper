package hopper

import (
	"fmt"
	"os"

	"go.etcd.io/bbolt"
)

const (
	defaultDBName = "default"
	ext           = "hopper"
)

type Map map[string]any

type Hopper struct {
	currentDatabase string
	*Options
	db *bbolt.DB
}

func New(options ...OptFunc) (*Hopper, error) {
	opts := &Options{
		Encoder: JSONEncoder{},
		Decoder: JSONDecoder{},
		DBName:  defaultDBName,
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
		currentDatabase: dbname,
		db:              db,
		Options:         opts,
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

func (h *Hopper) Coll(name string) *Filter {
	return NewFilter(h, name)
}
