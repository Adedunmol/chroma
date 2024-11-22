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

func (i *Insert) Parse(data map[string]interface{}) (*Insert, error) {

	ns := getNamespace(data)

	match, err := extractNamespace(ns)

	if err != nil {
		return i, err
	}

	i.Database = match[1]
	i.Table = match[2]
	i.Columns = i.getColumns(data)

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

func (i *Insert) getColumns(data map[string]interface{}) []KeyValue {

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
