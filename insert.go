package chroma

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
	tables         map[string]Table
	TypeError      = errors.New("unsupported type")
	NamespaceError = errors.New("invalid structure for namespace")
	namespace      = regexp.MustCompile("(\\w+)\\.(\\w+)")
	tableCreated   = false
	schemaCreated  = false
	mutex          *sync.Mutex
)

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
	columns := i.getColumns(data)

	i.Columns = columns

	return nil
}

func (i *Insert) String() string {

	var preStatement []string
	table, ok := tables[i.Table]

	if !ok {
		createdStr, err := i.CreateTable()
		if err != nil {
			panic(err)
		}
		preStatement = append(preStatement, createdStr)
	}

	if len(table.Schema) != len(i.Columns) {
		diff := i.getDifference(i.Columns)
		diffStr, err := i.assembleColumns(diff)
		if err != nil {
			panic(fmt.Errorf("could not assemble columns to alter table: %w", err))
		}

		i.Diff = diffStr
	}

	var columns []string
	var values []string

	for _, entry := range i.Columns {
		columns = append(columns, entry.Key)
		values = append(values, fmt.Sprintf("%v", entry.Value))

	}

	columnsStr := strings.Join(columns, ", ")
	valuesStr := strings.Join(values, ", ")

	insertStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", i.Table, columnsStr, valuesStr)

	if len(i.Diff) != 0 {
		alterStr := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", i.Table, strings.Join(i.Diff, " ")) + "\n"

		insertStr += alterStr
	}

	return insertStr
}

func extractNamespace(ns string) ([]string, error) {
	match := namespace.FindStringSubmatch(ns)

	if len(match) != 3 {
		return match, NamespaceError
	}

	return match, nil
}

func (i *Insert) getColumns(data map[string]interface{}) []KeyValue {

	var result []KeyValue

	object, ok := data["o"]

	if !ok {
		return result
	}

	table, ok := tables[i.Table]

	if !ok {
		panic("no table")
	}
	for key, value := range object.(map[string]interface{}) {
		table.Schema[key] = true
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
	//mutex.Lock()
	if !schemaCreated {
		schemaCreated = true

		schemaStr := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", i.Database)

		return schemaStr
	}
	//mutex.Unlock()

	return ""
}

func (i *Insert) CreateTable() (string, error) {
	//mutex.Lock()
	if !tableCreated {
		tableCreated = true
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
	//mutex.Unlock()

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
		panic("no table")
	}
	for _, entry := range columns {
		if _, ok := table.Schema[entry.Key]; !ok {
			data := KeyValue{Key: entry.Key, Value: entry.Value}
			result = append(result, data)
		}
	}

	return result
}
