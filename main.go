package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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

	const (
		parsersCount = 12
		writersCount = 4
	)

	start := time.Now()
	parserCh := make(chan []byte)
	writerCh := make(chan Record)

	var parsersWg sync.WaitGroup
	for i := 0; i < parsersCount; i++ {
		parsersWg.Add(1)
		go func() {
			for data := range parserCh {
				var key, value []byte
				keys := [][]string{
					[]string{"name"},
					[]string{"value"},
				}
				jsonparser.EachKey(data, func(idx int, val []byte, _ jsonparser.ValueType, err error) {
					if err != nil {
						log.Printf("parse index %d error: %v", idx, err)
						return
					}
					if idx == 0 {
						key = val
					} else if idx == 1 {
						value = val
					}
				}, keys...)
				if len(key) > 0 && len(value) > 0 {
					writerCh <- Record{
						Key:   reverse(string(key)),
						Value: copyBytes(value),
					}
				}
			}
			parsersWg.Done()
		}()
	}

	var writersWg sync.WaitGroup
	var totalRecords int64
	for i := 0; i < writersCount; i++ {
		writersWg.Add(1)
		go func() {
			records := make([]Record, 0, batchSize)
			for r := range writerCh {
				records = append(records, r)
				if len(records) == batchSize {
					if err := backend.Put(records); err != nil {
						log.Printf("Failed to put records: %v", err)
					}
					records = records[:0]
				}
				if cnt := atomic.AddInt64(&totalRecords, 1); cnt%1000000 == 0 {
					since := time.Since(start)
					log.Printf("Put %d records in %v (%.2f rps)", cnt, since, float64(cnt)/since.Seconds())
				}
			}
			if len(records) > 0 {
				if err := backend.Put(records); err != nil {
					log.Printf("Failed to put records: %v", err)
				}
			}
			writersWg.Done()
		}()
	}

	scanner := bufio.NewScanner(gr)
	log.Printf("Start loading file")
	for scanner.Scan() {
		parserCh <- copyBytes(scanner.Bytes())
	}

	close(parserCh)
	parsersWg.Wait()

	close(writerCh)
	writersWg.Wait()

	log.Printf("Loaded %d records in %v", totalRecords, time.Since(start))
	return scanner.Err()
}
