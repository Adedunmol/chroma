package chroma

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type KeyValue struct {
	Key   string
	Value interface{}
}

type Insert struct {
	Database string
	Table    string
	Columns  []KeyValue
}

var (
	NamespaceError = errors.New("invalid structure for namespace")
	namespace      = regexp.MustCompile("(\\w+)\\.(\\w+)")
)

func NewInsert() Insert {

	return Insert{}
}

func (i *Insert) Parse(data []byte) (*Insert, error) {

	oplog, err := ParseJSON(data)
	if err != nil {
		return i, fmt.Errorf("error parsing JSON: %s", err)
	}

	match, err := extractNamespace(oplog.Namespace)

	if err != nil {
		return i, err
	}

	i.Database = match[1]
	i.Table = match[2]
	i.Columns = i.getColumns(oplog)

	return i, nil
}

func (i *Insert) String() string {
	var columns []string
	var values []string

	for _, entry := range i.Columns {
		columns = append(columns, entry.Key)
		values = append(values, fmt.Sprintf("%v", entry.Value))

	}

	columnsStr := strings.Join(columns, ", ")
	valuesStr := strings.Join(values, ", ")

	insertStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", i.Table, columnsStr, valuesStr)

	return insertStr
}

func extractNamespace(ns string) ([]string, error) {
	match := namespace.FindStringSubmatch(ns)

	if len(match) != 3 {
		return match, NamespaceError
	}

	return match, nil
}

func (i *Insert) getColumns(oplog Oplog) []KeyValue {
	var result []KeyValue

	for key, value := range oplog.Object {
		data := KeyValue{Key: key, Value: value}
		result = append(result, data)
	}

	return result
}
