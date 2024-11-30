package main

import (
	"flag"
	"fmt"
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
