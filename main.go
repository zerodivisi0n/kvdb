package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
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

type JSONLine struct {
	Query string `json:"query"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	var (
		dbName           string
		backendType      string
		inputFilename    string
		query            string
		queryConcurrency int
		batchSize        int
		jsonFmt          bool
	)
	flag.StringVar(&dbName, "db", "", "Database name")
	flag.StringVar(&backendType, "backend", "badgerdb", "Database backend (leveldb, bbolt, badgerdb)")
	flag.StringVar(&inputFilename, "i", "", "Input filename")
	flag.StringVar(&query, "q", "", "Comma-separated query string")
	flag.IntVar(&queryConcurrency, "c", 10, "Query concurrency")
	flag.IntVar(&batchSize, "b", 5000, "Batch size")
	flag.BoolVar(&jsonFmt, "json", false, "Print output as json")
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

	queryParts := strings.Split(query, ",")
	queryTerms := queryParts[:0]
	for _, q := range queryParts {
		if len(q) > 0 {
			queryTerms = append(queryTerms, q)
		}
	}
	if len(queryTerms) > 0 {
		concurrency := queryConcurrency
		if l := len(queryTerms); l < concurrency {
			concurrency = l
		}
		type queryResult struct {
			query   string
			records []Record
			err     error
		}
		var results []queryResult
		if concurrency == 1 {
			for _, q := range queryTerms {
				records, err := backend.Search(reverse(q))
				results = append(results, queryResult{
					query:   q,
					records: records,
					err:     err,
				})
			}
		} else {
			queryCh := make(chan string)
			outCh := make(chan queryResult)
			var wg sync.WaitGroup
			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				go func() {
					for q := range queryCh {
						records, err := backend.Search(reverse(q))
						outCh <- queryResult{
							query:   q,
							records: records,
							err:     err,
						}
					}
					wg.Done()
				}()
			}

			go func() {
				for res := range outCh {
					results = append(results, res)
				}
			}()

			for _, q := range queryTerms {
				queryCh <- q
			}
			close(queryCh)
			wg.Wait()
			close(outCh)
		}
		for _, res := range results {
			if res.err != nil {
				log.Printf("Failed to search '%s': %v", res.query, res.err)
				continue
			}
			for _, r := range res.records {
				key := reverse(r.Key)
				if !jsonFmt {
					fmt.Printf("%s: %s\n", key, strings.TrimSpace(string(r.Value)))
				} else {
					output, err := json.Marshal(JSONLine{Query: res.query, Key: key, Value: string(r.Value)})
					if err != nil {
						log.Fatal(err)
					}
					fmt.Println(string(output))
				}
			}
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
