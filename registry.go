package quickstore

type registry struct {
	kinds map[string]bool
}

func (r *registry) Register(kind string) {
	r.kinds[kind] = true
}

var Registry = registry{
	kinds: make(map[string]bool),
}

