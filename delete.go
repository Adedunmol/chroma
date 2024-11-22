package chroma

import (
	"fmt"
)

type Delete struct {
	Database  string
	Table     string
	Condition KeyValue
}

func NewDelete() Delete {

	return Delete{}
}

func (d *Delete) Parse(data map[string]interface{}) error {

	ns := getNamespace(data)

	match, err := extractNamespace(ns)

	if err != nil {
		return err
	}

	d.Database = match[1]
	d.Table = match[2]
	d.Condition = d.getColumns(data)

	return nil
}

func (d *Delete) getColumns(data map[string]interface{}) KeyValue {

	var result []KeyValue

	object, ok := data["o"]

	if !ok {
		return KeyValue{}
	}

	for key, value := range object.(map[string]interface{}) {
		data := KeyValue{Key: key, Value: value}
		result = append(result, data)
	}

	return result[0]
}

func (d *Delete) String() string {

	conditionStr := fmt.Sprintf("%s = %v", d.Condition.Key, d.Condition.Value)

	insertStr := fmt.Sprintf("DELETE FROM %s WHERE %s", d.Table, conditionStr)

	return insertStr
}
