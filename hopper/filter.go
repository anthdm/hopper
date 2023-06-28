package hopper

import (
	"fmt"

	"go.etcd.io/bbolt"
)

const (
	FilterTypeEQ = "eq"
)

func eq(a, b any) bool {
	return a == b
}

type comparison func(a, b any) bool

type compFilter struct {
	kvs  Map
	comp comparison
}

func (f compFilter) apply(record Map) bool {
	for k, v := range f.kvs {
		value, ok := record[k]
		if !ok {
			return false
		}
		if k == "id" {
			return f.comp(value, uint64(v.(int)))
		}
		return f.comp(value, v)
	}
	return true
}

type Filter struct {
	hopper      *Hopper
	coll        string
	compFilters []compFilter
	slct        []string
	limit       int
}

func NewFilter(db *Hopper, coll string) *Filter {
	return &Filter{
		hopper:      db,
		coll:        coll,
		compFilters: make([]compFilter, 0),
	}
}

func (f *Filter) Eq(values Map) *Filter {
	filt := compFilter{
		comp: eq,
		kvs:  values,
	}
	f.compFilters = append(f.compFilters, filt)
	return f
}

// Insert insert the given values.
func (f *Filter) Insert(values Map) (uint64, error) {
	tx, err := f.hopper.db.Begin(true)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	collBucket, err := tx.CreateBucketIfNotExists([]byte(f.coll))
	if err != nil {
		return 0, err
	}
	id, err := collBucket.NextSequence()
	if err != nil {
		return 0, err
	}
	b, err := f.hopper.Encoder.Encode(values)
	if err != nil {
		return 0, err
	}
	if err := collBucket.Put(uint64Bytes(id), b); err != nil {
		return 0, err
	}
	return id, tx.Commit()
}

func (f *Filter) Find() ([]Map, error) {
	tx, err := f.hopper.db.Begin(true)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket([]byte(f.coll))
	if bucket == nil {
		return nil, fmt.Errorf("bucket (%s) not found", f.coll)
	}
	records, err := f.findFiltered(bucket)
	fmt.Println("records", records)
	if err != nil {
		return nil, err
	}
	return records, tx.Commit()
}

func (f *Filter) Update(values Map) ([]Map, error) {
	tx, err := f.hopper.db.Begin(true)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket([]byte(f.coll))
	if bucket == nil {
		return nil, fmt.Errorf("bucket (%s) not found", f.coll)
	}
	records, err := f.findFiltered(bucket)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		for k, v := range values {
			if _, ok := record[k]; ok {
				record[k] = v
			}
		}
		b, err := f.hopper.Encoder.Encode(record)
		if err != nil {
			return nil, err
		}
		if err := bucket.Put(uint64Bytes(record["id"].(uint64)), b); err != nil {
			return nil, err
		}
	}
	return records, tx.Commit()
}

func (f *Filter) Delete() error {
	tx, err := f.hopper.db.Begin(true)
	if err != nil {
		return err
	}
	bucket := tx.Bucket([]byte(f.coll))
	if bucket == nil {
		return fmt.Errorf("bucket (%s) not found", f.coll)
	}
	records, err := f.findFiltered(bucket)
	if err != nil {
		return err
	}
	for _, r := range records {
		idbytes := uint64Bytes(r["id"].(uint64))
		if err := bucket.Delete(idbytes); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (f *Filter) Limit(n int) *Filter {
	f.limit = n
	return f
}

func (f *Filter) Select(values ...string) *Filter {
	f.slct = append(f.slct, values...)
	return f
}

func (f *Filter) findFiltered(bucket *bbolt.Bucket) ([]Map, error) {
	results := []Map{}
	bucket.ForEach(func(k, v []byte) error {
		record := Map{
			"id": uint64FromBytes(k),
		}
		if err := f.hopper.Decoder.Decode(v, &record); err != nil {
			return err
		}
		include := true
		for _, filter := range f.compFilters {
			if !filter.apply(record) {
				include = false
				break
			}
		}
		if !include {
			return nil
		}
		record = f.applySelect(record)
		results = append(results, record)
		return nil
	})
	return results, nil
}

func (f *Filter) applySelect(record Map) Map {
	if len(f.slct) == 0 {
		return record
	}
	data := Map{}
	for _, key := range f.slct {
		if _, ok := record[key]; ok {
			data[key] = record[key]
		}
	}
	return data
}
