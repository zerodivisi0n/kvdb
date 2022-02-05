package main

import (
	"github.com/dgraph-io/badger/v3"
)

type BadgerDBBackend struct {
	db *badger.DB
}

func NewBadgerDBBackend(filename string) (*BadgerDBBackend, error) {
	opts := badger.DefaultOptions(filename).
		WithLoggingLevel(badger.WARNING)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerDBBackend{
		db: db,
	}, nil
}

func (b *BadgerDBBackend) Close() error {
	return b.db.Close()
}

func (b *BadgerDBBackend) Put(records []Record) error {
	wb := b.db.NewWriteBatch()
	for _, r := range records {
		if err := wb.Set([]byte(r.Key), r.Value); err != nil {
			return err
		}
	}

	return wb.Flush()
}

func (b *BadgerDBBackend) Search(prefix string) ([]Record, error) {
	var records []Record
	b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		bprefix := []byte(prefix)
		for it.Seek(bprefix); it.ValidForPrefix(bprefix); it.Next() {
			item := it.Item()
			key := item.Key()
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			records = append(records, Record{
				Key:   string(key),
				Value: value,
			})
		}
		return nil
	})
	return records, nil
}
