package hopper

import "fmt"

type UpdateFilter struct {
	kvs    Map
	hopper *Hopper
	coll   string
	eq     filter
}

func newUpdateFilter(h *Hopper, coll string) *UpdateFilter {
	return &UpdateFilter{
		hopper: h,
		coll:   coll,
	}
}

func (f *UpdateFilter) Values(kvs Map) *UpdateFilter {
	f.kvs = kvs
	return f
}

func (f *UpdateFilter) Eq(kvs Map) *UpdateFilter {
	f.eq = filter{
		comp: eq,
		kvs:  kvs,
	}
	return f
}

func (f *UpdateFilter) Exec() ([]Map, error) {
	tx, err := f.hopper.db.Begin(true)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket([]byte(f.coll))
	if bucket == nil {
		return nil, fmt.Errorf("bucket (%s) not found", f.coll)
	}
	filter := newFilter(f.hopper, f.coll)
	filter.filters = append(filter.filters, f.eq)
	records, err := filter.findFiltered(bucket)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		for k, v := range f.kvs {
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
