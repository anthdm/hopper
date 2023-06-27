package hopper

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
		if value, ok := record[k]; ok {
			if !f.comp(value, v) {
				return false
			}
		}
	}
	return true
}

type Filter struct {
	hopper      *Hopper
	coll        string
	compFilters []compFilter
}

func newFilter(db *Hopper, coll string) *Filter {
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
