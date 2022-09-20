package lattice

type L interface {
    Leq() bool
    Join() L
    Bot() L
    Diff() L
    Height() int
}

type IntSet map[int]struct{}

func (s *IntSet) Has(v int) bool {
	_, ok := s[v]
	return ok
}

func (s *IntSet) Add(v int) {
	s[v] = struct{}{}
}

func (s *IntSet) Height() int {
	return len(s)
}

func NewIntSet() *IntSet {
	s := &IntSet{}
	s = make(map[int]struct{})
	return s
}

func (s1 *IntSet) Join(s2 *IntSet) *IntSet {
	res := NewIntSet()
	for v := range s1 {
		res.Add(v)
	}

	for v := range s2 {
		res.Add(v)
	}
	return res
}

func (s1 *IntSet) Difference(s2 *IntSet) *IntSet {
	res := NewIntSet()
	for v := range s1 {
		if s2.Has(v) {
			continue
		}
		res.Add(v)
	}
	return res
}

func (s1 *IntSet).Leq(s2 *IntSet) bool {
    for v := range s1 {
        if !s2.Has(v) {
            return false
        }
    }
    return true
}


func (s1 *IntSet).Eq(s2 *IntSet) bool {
    if(s1.Height() != s2.Height()) {
        return false
    }
    return s1.Leq(s2)
}

func (s *IntSet).Bot() *IntSet {
    NewIntSet()
}
