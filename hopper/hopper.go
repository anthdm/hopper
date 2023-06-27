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
	EQ     Map
	Select []string
	Limit  int
	Sort   string
}

type Hopper struct {
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

func (h *Hopper) Update(coll string, filter Filter, data Map) ([]Map, error) {
	tx, err := h.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte(coll))
	if bucket == nil {
		return nil, fmt.Errorf("collection (%s) not found", coll)
	}
	records, err := h.findFiltered(bucket, filter)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		for k, v := range data {
			if k == "id" {
				continue
			}
			if _, ok := record[k]; ok {
				record[k] = v
			}
		}
		b, err := h.Encoder.Encode(record)
		if err != nil {
			return nil, err
		}
		id := record["id"].(uint64)
		if err := bucket.Put(uint64Bytes(id), b); err != nil {
			return nil, err
		}
	}
	return records, tx.Commit()
}

func (h *Hopper) findFiltered(bucket *bbolt.Bucket, filter Filter) ([]Map, error) {
	var records []Map
	bucket.ForEach(func(k, v []byte) error {
		record := Map{
			"id": uint64FromBytes(k),
		}
		if err := h.Decoder.Decode(v, &record); err != nil {
			return err
		}
		include := true
		if filter.EQ != nil {
			include = false
			for fk, fv := range filter.EQ {
				if value, ok := record[fk]; ok {
					if fv == value {
						include = true
					}
				}
			}
		}
		if include {
			if len(filter.Select) > 0 {
				data := Map{}
				for _, k := range filter.Select {
					data[k] = record[k]
				}
				records = append(records, data)
			} else {
				records = append(records, record)
			}
		}
		return nil
	})
	return records, nil
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
		record := Map{
			"id": uint64FromBytes(k),
		}
		if err := json.Unmarshal(v, &record); err != nil {
			return err
		}
		include := true
		if filter.EQ != nil {
			include = false
			for fk, fv := range filter.EQ {
				if value, ok := record[fk]; ok {
					if fv == value {
						include = true
					}
				}
			}
		}
		if include {
			if len(filter.Select) > 0 {
				data := Map{}
				for _, k := range filter.Select {
					data[k] = record[k]
				}
				results = append(results, data)
			} else {
				results = append(results, record)
			}
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
