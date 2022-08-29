package set

type IntSet map[int]struct{}

func (s *Set) Has(v int) bool {
	_, ok := s[v]
	return ok
}

func (s *Set) Add(v int) {
	s[v] = struct{}{}
}

func (s *Set) Remove(v int) {
	delete(s, v)
}

func (s *Set) Clear() {
	s = make(map[int]struct{})
}

func (s *Set) Size() int {
	return len(s)
}

func NewSet() *Set {
	s := &Set{}
	s = make(map[int]struct{})
	return s
}


//AddMulti Add multiple values in the set
func (s *Set) AddMulti(list ...int) {
	for _, v := range list {
		s.Add(v)
	}
}

type FilterFunc func(v int) bool

// Filter returns a subset, that contains only the values that satisfies the given predicate P
func (s *Set) Filter(P FilterFunc) *Set {
	res := NewSet()
	for v := range s {
		if P(v) == false {
			continue
		}
		res.Add(v)
	}
	return res
}

func (s *Set) Union(s2 *Set) *Set {
	res := NewSet()
	for v := range s {
		res.Add(v)
	}

	for v := range s2 {
		res.Add(v)
	}
	return res
}

func (s *Set) Intersect(s2 *Set) *Set {
	res := NewSet()
	for v := range s {
		if s2.Has(v) == false {
			continue
		}
		res.Add(v)
	}
	return res
}

// Difference returns the subset from s, that doesn't exists in s2 (param)
func (s *Set) Difference(s2 *Set) *Set {
	res := NewSet()
	for v := range s {
		if s2.Has(v) {
			continue
		}
		res.Add(v)
	}
	return res
}
