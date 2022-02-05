package main

type DummyBackend struct{}

func NewDummyBackend() *DummyBackend {
	return &DummyBackend{}
}

func (b *DummyBackend) Put(records []Record) error {
	return nil
}

func (b *DummyBackend) Search(prefix string) ([]Record, error) {
	return nil, nil
}

func (b *DummyBackend) Close() error {
	return nil
}
