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

var NamespaceError = errors.New("invalid structure for namespace")

var namespace = regexp.MustCompile("(\\w+)\\.(\\w+)")

func NewInsert() Insert {

	return Insert{}
}

func (i *Insert) Parse(data []byte) (Insert, error) {
	var insert Insert
	oplog, err := ParseJSON(data)
	if err != nil {
		return Insert{}, fmt.Errorf("error parsing JSON: %s", err)
	}

	match := namespace.FindStringSubmatch(oplog.Namespace)

	if len(match) != 3 {
		return Insert{}, NamespaceError
	}
	insert.Database = match[1]
	insert.Table = match[2]
	insert.Columns = insert.getColumns(oplog)

	return insert, nil
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

func (i *Insert) getColumns(oplog Oplog) []KeyValue {
	var result []KeyValue

	for key, value := range oplog.Object {
		data := KeyValue{Key: key, Value: value}
		result = append(result, data)
	}

	return result
}
