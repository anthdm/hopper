package hopper

import (
	"fmt"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
)

const (
	defaultDBName = "default"
)

type M map[string]string

type Filter struct {
	EQ    map[string]any
	Limit int
	Sort  string
}

type Collection struct {
	*bbolt.Bucket
}

type Hopper struct {
	db *bbolt.DB
}

func New() (*Hopper, error) {
	dbname := fmt.Sprintf("%s.hopper", defaultDBName)
	db, err := bbolt.Open(dbname, 0666, nil)
	if err != nil {
		return nil, err
	}
	return &Hopper{
		db: db,
	}, nil
}

func (h *Hopper) CreateCollection(name string) (*Collection, error) {
	tx, err := h.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket, err := tx.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return nil, err
	}
	return &Collection{Bucket: bucket}, nil
}

func (h *Hopper) Insert(collName string, data M) (uuid.UUID, error) {
	id := uuid.New()
	tx, err := h.db.Begin(true)
	if err != nil {
		return id, err
	}
	defer tx.Rollback()

	collBucket, err := tx.CreateBucketIfNotExists([]byte(collName))
	if err != nil {
		return id, err
	}
	recordBucket, err := collBucket.CreateBucket([]byte(id.String()))
	if err != nil {
		return id, nil
	}
	for k, v := range data {
		if err := recordBucket.Put([]byte(k), []byte(v)); err != nil {
			return id, err
		}
	}
	return id, tx.Commit()
}

func (h *Hopper) Find(coll string, filter Filter) ([]M, error) {
	tx, err := h.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte(coll))
	if bucket == nil {
		return nil, fmt.Errorf("collection (%s) not found", coll)
	}
	results := []M{}
	bucket.ForEach(func(k, v []byte) error {
		if v == nil {
			entryBucket := bucket.Bucket(k)
			if entryBucket == nil {
				return fmt.Errorf("entry found without field data")
			}
			data := M{}
			entryBucket.ForEach(func(k, v []byte) error {
				data[string(k)] = string(v)
				return nil
			})
			include := true
			if filter.EQ != nil {
				include = false
				for fk, fv := range filter.EQ {
					if value, ok := data[fk]; ok {
						if value == fv {
							include = true
						}
					}
				}
			}
			if include {
				results = append(results, data)
			}
		}
		return nil
	})
	return results, tx.Commit()
}
