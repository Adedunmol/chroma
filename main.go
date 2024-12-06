package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
)

type Handler interface {
	Parse(map[string]interface{}) error
	String() string
}

const WORKERS = 5

var (
	wg          sync.WaitGroup
	NoFileFound = errors.New("no file found")
	input       = flag.String("i", "", "input file")
	output      = flag.String("o", "", "output file")
)

type Options struct {
	Input  string
	Output string
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: chroma [-i input file] [-o output file]\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
	}

	if err := run(Options{Input: *input, Output: *output}); err != nil {
		fmt.Fprintln(os.Stderr, errors.Unwrap(err))
	}

	wg.Wait()
}

func run(options Options) error {
	opsChan := make(chan map[string]interface{}, WORKERS*2)
	queryOutputChan := make(chan string, WORKERS*2)

	if options.Input == "" {
		return NoFileFound
	}

	if options.Output == "" {
		options.Output = "output.sql"
	}

	fileData, err := openFile(os.DirFS("."), options.Input)
	if err != nil {
		return err
	}

	fileHandle, err := os.Create(options.Output)
	if err != nil {
		return err
	}

	oplogs, err := ParseJSONArray(fileData)
	if err != nil {
		return err
	}

	wg.Add(1)
	go writeOutputQuery(fileHandle, queryOutputChan)

	wg.Add(WORKERS)
	for i := 0; i < WORKERS; i++ {
		go worker(opsChan, queryOutputChan)
	}

	for _, op := range oplogs {
		opsChan <- op
	}

	return nil
}

func openFile(fileSystem fs.FS, name string) ([]byte, error) {
	file, err := fileSystem.Open(name)

	defer file.Close()

	if err != nil {
		return nil, fmt.Errorf("error opening file %s: %w", name, err)
	}

	data, err := io.ReadAll(file)

	if err != nil {
		return nil, fmt.Errorf("error reading file %s: %w", name, err)
	}

	return data, nil
}

func SeparateOperations(oplogs []map[string]interface{}) []Handler {
	var handlers []Handler

	for _, oplog := range oplogs {
		switch oplog["op"] {
		case "insert":
			insert := NewInsert()
			err := insert.Parse(oplog)

			if err != nil {
				panic(errors.Unwrap(err))
			}
			handlers = append(handlers, &insert)
			break
		case "update":
			update := NewUpdate()
			err := update.Parse(oplog)

			if err != nil {
				panic(errors.Unwrap(err))
			}
			handlers = append(handlers, &update)
			break
		case "delete":
			deleteOp := NewDelete()
			err := deleteOp.Parse(oplog)

			if err != nil {
				panic(errors.Unwrap(err))
			}
			handlers = append(handlers, &deleteOp)
			break
		default:
			panic(fmt.Errorf("unknown oplog type: %s", oplog["op"]))
		}
	}

	return handlers
}

func worker(ops chan map[string]interface{}, output chan string) {
	defer wg.Done()
	for op := range ops {
		switch op["op"] {
		case "insert":
			insert := NewInsert()
			err := insert.Parse(op)

			if err != nil {
				panic(errors.Unwrap(err))
			}
			result := insert.String()
			output <- result
			break
		case "update":
			update := NewUpdate()
			err := update.Parse(op)

			if err != nil {
				panic(errors.Unwrap(err))
			}

			result := update.String()
			output <- result
			break
		case "delete":
			deleteOp := NewDelete()
			err := deleteOp.Parse(op)

			if err != nil {
				panic(errors.Unwrap(err))
			}

			result := deleteOp.String()
			output <- result
			break
		default:
			panic(fmt.Errorf("unknown oplog type: %s", op["op"]))
		}
	}

}

func writeOutputQuery(file *os.File, queryChan chan string) {
	defer wg.Done()
	for query := range queryChan {
		_, err := file.WriteString(query + "\n")
		if err != nil {
			panic(errors.Unwrap(err))
		}
	}
}
