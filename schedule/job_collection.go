package main

func JobsCopy(p []*Job) (r []*Job) {
	if p == nil {
		return nil
	}

	r = make([]*Job, len(p))
	for i, v := range p {
		r[i] = v
	}

	return r
}
