package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
)

type Handler interface {
	Parse(map[string]interface{}) error
	String() string
}

const WOKRERS uint8 = 10

var (
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
}

func run(options Options) error {

	if options.Output == "" {
		return NoFileFound
	}

	if options.Input == "" {
		return NoFileFound
	}

	fileData, err := openFile(os.DirFS("."), options.Input)
	if err != nil {
		return err
	}

	oplogs, err := ParseJSONArray(fileData)
	if err != nil {
		return err
	}

	_ = SeparateOperations(oplogs)

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
