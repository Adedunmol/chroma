package main

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
)

type KeyValue struct {
	Key   string
	Value interface{}
}

type Table struct {
	Name   string
	Schema map[string]bool
}

type Insert struct {
	Database string
	Table    string
	Columns  []KeyValue
	Diff     []string
}

var (
	tables         = make(map[string]Table)
	schemas        = make(map[string]bool)
	TypeError      = errors.New("unsupported type")
	NamespaceError = errors.New("invalid structure for namespace")
	namespace      = regexp.MustCompile("(\\w+)\\.(\\w+)")
	mutex          *sync.Mutex
)

func GetTable(name string) bool {
	_, ok := tables[name]

	if !ok {
		return false
	}

	return true
}

func GetSchema(name string) bool {
	_, ok := schemas[name]

	if !ok {
		return false
	}

	return true
}

func NewInsert() Insert {

	return Insert{}
}

func (i *Insert) Parse(data map[string]interface{}) error {

	ns := getNamespace(data)

	match, err := extractNamespace(ns)

	if err != nil {
		return err
	}

	i.Database = match[1]
	i.Table = match[2]
	columns := i.getEntries(data)

	i.Columns = columns

	return nil
}

func (i *Insert) String() string {

	preStatements := i.prependStatements()

	var columns []string
	var values []string

	for _, entry := range i.Columns {
		columns = append(columns, entry.Key)
		values = append(values, fmt.Sprintf("%v", entry.Value))

	}

	columnsStr := strings.Join(columns, ", ")
	valuesStr := strings.Join(values, ", ")

	insertStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", i.Table, columnsStr, valuesStr)

	result := strings.Join(preStatements, "") + insertStr

	return result
}

func (i *Insert) prependStatements() []string {
	var preStatements []string

	_, ok := schemas[i.Database]

	if !ok {
		schemaStr := i.CreateSchema()

		preStatements = append(preStatements, schemaStr+"\n")
	}

	table, ok := tables[i.Table]

	if !ok {
		createTableStr, err := i.CreateTable()
		if err != nil {
			panic(err)
		}
		preStatements = append(preStatements, createTableStr+"\n")
	}

	if len(table.Schema) != len(i.Columns) {
		diff := i.getDifference(i.Columns)
		diffStr, err := i.assembleColumns(diff)
		if err != nil {
			panic(fmt.Errorf("could not assemble columns to alter table(%s): %w", i.Table, err))
		}

		if len(diffStr) != 0 {
			alterStr := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", i.Table, strings.Join(i.Diff, " ")) + "\n"

			preStatements = append(preStatements, alterStr+"\n")
		}
	}

	return preStatements
}

func extractNamespace(ns string) ([]string, error) {
	match := namespace.FindStringSubmatch(ns)

	if len(match) != 3 {
		return match, NamespaceError
	}

	return match, nil
}

func (i *Insert) getEntries(data map[string]interface{}) []KeyValue {

	var result []KeyValue

	object, ok := data["o"]

	if !ok {
		return result
	}

	for key, value := range object.(map[string]interface{}) {
		data := KeyValue{Key: key, Value: value}
		result = append(result, data)
	}

	return result
}

func getNamespace(data map[string]interface{}) string {

	ns, exists := data["ns"]
	if !exists {
		return ""
	}

	return ns.(string)
}

func (i *Insert) CreateSchema() string {

	mutex.Lock()
	{

		_, ok := schemas[i.Database]

		if !ok {
			schemas[i.Database] = true

			schemaStr := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", i.Database)

			return schemaStr
		}
	}
	mutex.Unlock()

	return ""
}

func (i *Insert) CreateTable() (string, error) {

	mutex.Lock()
	{

		_, ok := tables[i.Table]

		if !ok {

			tables[i.Table] = Table{Name: i.Table, Schema: make(map[string]bool)}

			for _, column := range i.Columns {
				tables[i.Table].Schema[column.Key] = true
			}

			tableStr := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", i.Table)
			columns, err := i.assembleColumns(i.Columns)
			if err != nil {
				return "", err
			}
			columnsStr := strings.Join(columns, "\n")

			tableStr += columnsStr + "\n"

			tableStr += ");"

			return tableStr, nil
		}
	}
	mutex.Unlock()

	return "", nil
}

func (i *Insert) assembleColumns(columns []KeyValue) ([]string, error) {
	var result []string

	for idx, entry := range columns {
		var colEntry []string

		colEntry = append(colEntry, "\t")
		colEntry = append(colEntry, entry.Key)

		switch reflect.TypeOf(entry.Value).Kind() {
		case reflect.String:
			colEntry = append(colEntry, "VARCHAR(255)")
			break
		case reflect.Int:
			colEntry = append(colEntry, "BIGINT")
		case reflect.Float64:
			colEntry = append(colEntry, "FLOAT")
		case reflect.Bool:
			colEntry = append(colEntry, "BOOLEAN")
		default:
			return result, TypeError
		}

		if entry.Key == "_id" {
			colEntry = append(colEntry, "PRIMARY KEY")
		}

		col := strings.Join(colEntry, " ")

		if idx != len(i.Columns)-1 {
			col += ","
		}

		result = append(result, col)
	}

	return result, nil
}

func (i *Insert) getDifference(columns []KeyValue) []KeyValue {
	var result []KeyValue

	table, ok := tables[i.Table]

	if !ok {
		panic(fmt.Sprintf("no table: %s", i.Table))
	}
	for _, entry := range columns {
		if _, ok := table.Schema[entry.Key]; !ok {
			data := KeyValue{Key: entry.Key, Value: entry.Value}
			result = append(result, data)
		}
	}

	return result
}
