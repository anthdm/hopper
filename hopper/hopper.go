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
	b, err := h.Encoder.Encode(data)
	if err != nil {
		return 0, err
	}
	if err := collBucket.Put(uint64Bytes(id), b); err != nil {
		return 0, err
	}
	return id, tx.Commit()
}

func (h *Hopper) Update(coll string, filter Filter, data Map) ([]Map, error) {
	// tx, err := h.db.Begin(true)
	// if err != nil {
	// 	return nil, err
	// }
	// defer tx.Rollback()

	// bucket := tx.Bucket([]byte(coll))
	// if bucket == nil {
	// 	return nil, fmt.Errorf("collection (%s) not found", coll)
	// }
	// records, err := h.findFiltered(bucket, filter)
	// if err != nil {
	// 	return nil, err
	// }
	// for _, record := range records {
	// 	for k, v := range data {
	// 		if k == "id" {
	// 			continue
	// 		}
	// 		if _, ok := record[k]; ok {
	// 			record[k] = v
	// 		}
	// 	}
	// 	b, err := h.Encoder.Encode(record)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	id := record["id"].(uint64)
	// 	if err := bucket.Put(uint64Bytes(id), b); err != nil {
	// 		return nil, err
	// 	}
	// }
	// return records, tx.Commit()
	return nil, nil
}

func (h *Hopper) Find(collname string) *Filter {
	return newFilter(h, collname)
}
