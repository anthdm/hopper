package hopper

import "fmt"

type UpdateFilter struct {
	filter *Filter
	kvs    Map
}

func NewUpdateFilter(f *Filter) *UpdateFilter {
	return &UpdateFilter{
		filter: f,
	}
}

func (f *UpdateFilter) Eq(kvs Map) *UpdateFilter {
	f.filter.Eq(kvs)
	return f
}

func (f *UpdateFilter) Values(kvs Map) *UpdateFilter {
	f.kvs = kvs
	return f
}

func (f *UpdateFilter) Exec() ([]Map, error) {
	tx, err := f.filter.hopper.db.Begin(true)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket([]byte(f.filter.coll))
	if bucket == nil {
		return nil, fmt.Errorf("bucket (%s) not found", f.filter.coll)
	}
	records, err := NewFindFilter(f.filter).findFiltered(bucket)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		for k, v := range f.kvs {
			if _, ok := record[k]; ok {
				record[k] = v
			}
		}
		b, err := f.filter.hopper.Encoder.Encode(record)
		if err != nil {
			return nil, err
		}
		if err := bucket.Put(uint64Bytes(record["id"].(uint64)), b); err != nil {
			return nil, err
		}
	}
	return records, tx.Commit()
}
