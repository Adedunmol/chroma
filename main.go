package main

import (
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

var (
	input  = flag.String("i", "", "input file")
	output = flag.String("o", "", "output file")
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
		fmt.Fprintln(os.Stderr, err)
	}
}

func run(options Options) error {

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

func separateOperations(oplogs []map[string]interface{}) []Handler {
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
