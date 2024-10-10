package registry

import (
	"errors"
	"hash"
	"sync"
)

const (
	expectedBucketsNumber = 6
)

type (
	Server struct {
		Address string
	}

	Registry struct {
		servers []*Server
		lock    sync.Mutex
	}
)

func NewRegistry() *Registry {
	return &Registry{
		servers: make([]*Server, 0, expectedBucketsNumber),
	}
}

func (r *Registry) Register(s *Server) error {
	if len(s.Address) == 0 {
		return errors.New("no address")
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	for _, server := range r.servers {
		if server.Address == s.Address {
			return errors.New("server is already registered")
		}
	}
	r.servers = append(r.servers, s)
	return nil
}

func (r *Registry) GetServer(chunkHash hash.Hash64) *Server {
	r.lock.Lock()
	defer r.lock.Unlock()

	if len(r.servers) == 0 {
		return nil
	}

	idx := chunkHash.Sum64() % uint64(len(r.servers))

	return r.servers[idx]
}

