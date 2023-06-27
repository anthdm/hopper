package hopper

import (
	"fmt"

	"go.etcd.io/bbolt"
)

type FindFilter struct {
	filter *Filter
	slct   []string
	limit  int
}

func NewFindFilter(f *Filter) *FindFilter {
	return &FindFilter{
		filter: f,
		slct:   []string{},
	}
}

func (f *FindFilter) Eq(kvs Map) *FindFilter {
	f.filter.Eq(kvs)
	return f
}

func (f *FindFilter) Exec() ([]Map, error) {
	tx, err := f.filter.hopper.db.Begin(true)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket([]byte(f.filter.coll))
	if bucket == nil {
		return nil, fmt.Errorf("bucket (%s) not found", f.filter.coll)
	}
	records, err := f.findFiltered(bucket)
	if err != nil {
		return nil, err
	}
	return records, tx.Commit()
}

func (f *FindFilter) Select(keys ...string) *FindFilter {
	f.slct = append(f.slct, keys...)
	return f
}

func (f *FindFilter) Limit(n int) *FindFilter {
	f.limit = n
	return f
}

func (f *FindFilter) findFiltered(bucket *bbolt.Bucket) ([]Map, error) {
	results := []Map{}
	bucket.ForEach(func(k, v []byte) error {
		record := Map{
			"id": uint64FromBytes(k),
		}
		if err := f.filter.hopper.Decoder.Decode(v, &record); err != nil {
			return err
		}
		include := true
		for _, filter := range f.filter.compFilters {
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

func (f *FindFilter) applySelect(record Map) Map {
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
