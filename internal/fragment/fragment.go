package fragment

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type (
	UploadStatus = int

	FileMeta struct {
		Status    UploadStatus `json:"status"`
		Name      string       `json:"filename"`
		Addresses []string     `json:"addresses"` // index is fragment number
	}

	Registry struct {
		lock  sync.Mutex
		Files map[string]*FileMeta `json:"files"`
	}
)

const (
	fragmentsFile = "fragments.json"

	UploadStatusIncomplete UploadStatus = iota
	UploadStatusComplete
	UploadStatusFailed
)

func NewRegistry() (*Registry, error) {
	f, err := os.ReadFile(fragmentsFile)

	if os.IsNotExist(err) {
		return &Registry{
			Files: make(map[string]*FileMeta),
		}, nil
	}

	if err != nil {
		return nil, err
	}

	var r Registry
	err = json.Unmarshal(f, &r)
	return &r, err
}

func (r *Registry) store() error {
	f, err := json.MarshalIndent(r, "", " ")
	if err != nil {
		return err
	}

	return os.WriteFile(fragmentsFile, f, 0644)
}

func (r *Registry) AddFragment(filename, address string) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	fm, ok := r.Files[filename]
	if !ok {
		r.Files[filename] = &FileMeta{
			Name:      filename,
			Addresses: []string{address},
		}
	} else {
		fm.Addresses = append(fm.Addresses, address)
	}

	return r.store()
}

func (r *Registry) SetStatus(filename string, status UploadStatus) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	fm, ok := r.Files[filename]
	if !ok {
		return fmt.Errorf("no fragments of %s file", filename)
	}

	fm.Status = status

	return r.store()
}

func (r *Registry) Delete(filename string) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	delete (r.Files, filename)

	return r.store()
}
