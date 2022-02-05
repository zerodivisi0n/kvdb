package main

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDBBackend struct {
	db *leveldb.DB
}

func NewLevelDBBackend(filename string) (*LevelDBBackend, error) {
	db, err := leveldb.OpenFile(filename, nil)
	if err != nil {
		return nil, err
	}
	return &LevelDBBackend{
		db: db,
	}, nil
}

// best batch size: 50000
func (b *LevelDBBackend) Put(records []Record) error {
	batch := leveldb.Batch{}
	for _, r := range records {
		batch.Put([]byte(r.Key), r.Value)
	}
	return b.db.Write(&batch, nil) // write options do not change anything
}

func (b *LevelDBBackend) Search(prefix string) ([]Record, error) {
	var records []Record
	iter := b.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	for iter.Next() {
		records = append(records, Record{
			Key:   string(iter.Key()),
			Value: copyBytes(iter.Value()),
		})
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return records, nil
}

func (b *LevelDBBackend) Close() error {
	return b.db.Close()
}
