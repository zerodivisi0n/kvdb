package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/buger/jsonparser"
)

type Record struct {
	Key   string
	Value []byte
}

type Backend interface {
	Put(records []Record) error
	Search(prefix string) ([]Record, error)
	Close() error
}

func main() {
	var (
		dbName        string
		backendType   string
		inputFilename string
		query         string
		batchSize     int
	)
	flag.StringVar(&dbName, "db", "", "Database name")
	flag.StringVar(&backendType, "backend", "badgerdb", "Database backend (leveldb, bbolt, badgerdb)")
	flag.StringVar(&inputFilename, "i", "", "Input filename")
	flag.StringVar(&query, "q", "", "Query subdomains")
	flag.IntVar(&batchSize, "b", 5000, "Batch size")
	flag.Parse()

	if dbName == "" {
		fmt.Fprintln(os.Stderr, "Missing db flag")
		flag.Usage()
		os.Exit(1)
	}

	var backend Backend
	var err error
	switch backendType {
	case "dummy":
		backend = NewDummyBackend()
	case "leveldb":
		backend, err = NewLevelDBBackend(dbName + ".ldb")
	case "bbolt":
		backend, err = NewBBoltBackend(dbName + ".bbolt")
	case "badgerdb":
		backend, err = NewBadgerDBBackend(dbName + ".badger")
	default:
		err = fmt.Errorf("invalid backend")
	}
	if err != nil {
		log.Panicf("Failed to open file: %v", err)
	}

	defer backend.Close()

	if inputFilename != "" {
		if err := loadFile(backend, inputFilename, batchSize); err != nil {
			log.Fatal(err)
		}
	}

	if query != "" {
		records, err := backend.Search(reverse(query))
		if err != nil {
			log.Fatalf("Failed to search: %v", err)
		}
		for _, r := range records {
			fmt.Printf("%s: %s\n", reverse(r.Key), strings.TrimSpace(string(r.Value)))
		}
	}
}

func loadFile(backend Backend, filename string, batchSize int) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gr.Close()

	scanner := bufio.NewScanner(gr)
	i := 0
	records := make([]Record, 0, batchSize)
	start := time.Now()
	log.Printf("Start loading file")
	for scanner.Scan() {
		var key, value []byte
		keys := [][]string{
			[]string{"name"},
			[]string{"value"},
		}
		var errs []error
		jsonparser.EachKey(scanner.Bytes(), func(idx int, val []byte, _ jsonparser.ValueType, err error) {
			if err != nil {
				errs = append(errs, err)
				return
			}
			if idx == 0 {
				key = val
			} else if idx == 1 {
				value = val
			}
		}, keys...)
		if errs != nil {
			return fmt.Errorf("multiple errors: %v", errs)
		}

		records = append(records, Record{
			Key:   reverse(string(key)),
			Value: copyBytes(value),
		})
		i++
		if len(records) == batchSize {
			if err := backend.Put(records); err != nil {
				return err
			}
			records = records[:0]
		}
		if i%1000000 == 0 {
			log.Printf("Put %d records in %v", i, time.Since(start))
		}
	}

	if len(records) > 0 {
		if err := backend.Put(records); err != nil {
			return err
		}
	}

	log.Printf("Load complete in %v", time.Since(start))
	return scanner.Err()
}
