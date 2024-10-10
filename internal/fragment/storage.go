package fragment

import (
	"errors"
	"fmt"
	filesystem "io/fs"
	"os"
	"path"
)

type (
	Storage struct {
		directory string
	}
)

func NewFragmentsStorage(directory string) (*Storage, error) {
	err := os.MkdirAll(directory, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &Storage{
		directory: directory,
	}, nil
}

func (fs *Storage) fragmentPath(filename string, fragment int) string {
	return path.Join(fs.directory, fmt.Sprintf("%s_%d.bin", filename, fragment))
}

func (fs *Storage) Put(filename string, fragment int, data []byte) error {
	f := fs.fragmentPath(filename, fragment)
	_, err := os.Stat(f)
	if !errors.Is(err, filesystem.ErrNotExist) {
		return errors.New("fragment is already stored")
	}

	err = os.WriteFile(f, data, 0644)
	if err != nil {
		return err
	}

	return nil
}

func (fs *Storage) Get(filename string, fragment int) (*os.File, error) {
	file, err := os.Open(fs.fragmentPath(filename, fragment))
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (fs *Storage) Delete(filename string, fragment int) error {
	return os.Remove(fs.fragmentPath(filename, fragment))
}
