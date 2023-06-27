package hopper

import (
	"fmt"

	"go.etcd.io/bbolt"
)

func eq(a, b any) bool {
	return a == b
}

type comparison func(a, b any) bool

type filter struct {
	kvs  Map
	comp comparison
}

func (f filter) exec(record Map) bool {
	for k, v := range f.kvs {
		if value, ok := record[k]; ok {
			if !f.comp(value, v) {
				return false
			}
		}
	}
	return true
}

type Filter struct {
	hopper  *Hopper
	coll    string
	limit   int
	slct    []string
	filters []filter
}

func newFilter(db *Hopper, coll string) *Filter {
	return &Filter{
		hopper:  db,
		coll:    coll,
		slct:    make([]string, 0),
		filters: make([]filter, 0),
	}
}

func (f *Filter) Eq(values Map) *Filter {
	filt := filter{
		comp: eq,
		kvs:  values,
	}
	f.filters = append(f.filters, filt)
	return f
}

func (f *Filter) Select(keys ...string) *Filter {
	f.slct = append(f.slct, keys...)
	return f
}

func (f *Filter) Limit(n int) *Filter {
	f.limit = n
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
		for _, filter := range f.filters {
			if !filter.exec(record) {
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

func (f *Filter) Exec() ([]Map, error) {
	tx, err := f.hopper.db.Begin(true)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket([]byte(f.coll))
	if bucket == nil {
		return nil, fmt.Errorf("bucket (%s) not found", f.coll)
	}
	return f.findFiltered(bucket)
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
