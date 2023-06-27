package hopper

import "fmt"

type DeleteFilter struct {
	filter *Filter
}

func NewDeleteFilter(f *Filter) *DeleteFilter {
	return &DeleteFilter{
		filter: f,
	}
}

func (f *DeleteFilter) Eq(kvs Map) *DeleteFilter {
	f.filter.Eq(kvs)
	return f
}

func (f *DeleteFilter) Exec() error {
	tx, err := f.filter.hopper.db.Begin(true)
	if err != nil {
		return err
	}
	bucket := tx.Bucket([]byte(f.filter.coll))
	if bucket == nil {
		return fmt.Errorf("bucket (%s) not found", f.filter.coll)
	}
	records, err := NewFindFilter(f.filter).findFiltered(bucket)
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
