package main

import (
	"bytes"

	"go.etcd.io/bbolt"
)

type BBoltBackend struct {
	db *bbolt.DB
}

func NewBBoltBackend(filename string) (*BBoltBackend, error) {
	db, err := bbolt.Open(filename, 0666, nil)
	if err != nil {
		return nil, err
	}
	return &BBoltBackend{
		db: db,
	}, nil
}

func (b *BBoltBackend) Put(records []Record) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("bucket"))
		if err != nil {
			return err
		}
		for _, r := range records {
			if err := bucket.Put([]byte(r.Key), r.Value); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BBoltBackend) Search(prefix string) ([]Record, error) {
	var records []Record
	err := b.db.View(func(tx *bbolt.Tx) error {
		// Assume bucket exists and has keys
		c := tx.Bucket([]byte("bucket")).Cursor()

		bprefix := []byte(prefix)
		for k, v := c.Seek(bprefix); k != nil && bytes.HasPrefix(k, bprefix); k, v = c.Next() {
			records = append(records, Record{
				Key:   string(k),
				Value: v,
			})
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (b *BBoltBackend) Close() error {
	return b.db.Close()
}
