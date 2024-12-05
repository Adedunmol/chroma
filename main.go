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

const WOKRERS = 5

var (
	wg          sync.WaitGroup
	NoFileFound = errors.New("no file found")
	input       = flag.String("i", "", "input file")
	output      = flag.String("o", "", "output file")
	concurrent  = flag.Bool("c", true, "concurrent goroutines")
)

type Options struct {
	Input      string
	Output     string
	Concurrent bool
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

	if err := run(Options{Input: *input, Output: *output, Concurrent: *concurrent}); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}

	wg.Wait()
}

func run(options Options) error {
	opsChan := make(chan Handler, WOKRERS)
	queryOutputChan := make(chan string)

	if options.Output == "" {
		return NoFileFound
	}

	if options.Input == "" {
		options.Input = "output.sql"
	}

	fileData, err := openFile(os.DirFS("."), options.Input)
	if err != nil {
		return err
	}

	oplogs, err := ParseJSONArray(fileData)
	if err != nil {
		return err
	}

	ops := SeparateOperations(oplogs)

	fileHandle, err := os.Create(options.Output)
	if err != nil {
		return err
	}

	wg.Add(1)
	go writeOutputQuery(fileHandle, queryOutputChan)

	wg.Add(WOKRERS)
	for i := 0; i < WOKRERS; i++ {
		go worker(opsChan, queryOutputChan)
	}

	for _, op := range ops {
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
				panic(err)
			}
			handlers = append(handlers, &insert)
			break
		case "update":
			update := NewUpdate()
			err := update.Parse(oplog)

			if err != nil {
				panic(err)
			}
			handlers = append(handlers, &update)
			break
		case "delete":
			delete := NewDelete()
			err := delete.Parse(oplog)

			if err != nil {
				panic(err)
			}
			handlers = append(handlers, &delete)
			break
		default:
			panic(fmt.Errorf("unknown oplog type: %s", oplog["op"]))
		}
	}

	return handlers
}

func worker(ops chan Handler, output chan string) {
	defer wg.Done()
	for op := range ops {
		result := op.String()
		output <- result
	}

}

func writeOutputQuery(file *os.File, queryChan chan string) {
	defer wg.Done()
	for query := range queryChan {
		_, err := file.WriteString(query + "\n")
		if err != nil {
			panic(err)
		}
	}
}
