package main

import (
	"errors"
	"fmt"
	"strings"
)

type Update struct {
	Op        string
	Database  string
	Table     string
	Columns   []KeyValue
	Condition KeyValue
}

func NewUpdate() Update {

	return Update{}
}

func (u *Update) Parse(data map[string]interface{}) error {

	ns := getNamespace(data)

	match, err := extractNamespace(ns)

	if err != nil {
		return err
	}

	u.Database = match[1]
	u.Table = match[2]

	op, err := getOperation(data)
	if err != nil {
		return err
	}

	u.Op = op

	u.Columns = u.getColumns(data, u.Op)

	query, err := u.getCondition(data)

	if err != nil {
		return err
	}
	u.Condition = query

	return nil
}

func (u *Update) String() string {
	var columns []string
	var updateStr string

	for _, c := range u.Columns {
		if u.Op == "u" {
			columns = append(columns, fmt.Sprintf("%s = %v", c.Key, c.Value))
		} else {
			val := c.Value.(bool)
			var value string

			if !val {
				value = "NULL"
			}

			columns = append(columns, fmt.Sprintf("%s = %s", c.Key, value))
		}
	}

	columnsStr := strings.Join(columns, ", ")
	conditionStr := fmt.Sprintf("%s = %s", u.Condition.Key, u.Condition.Value)

	updateStr = fmt.Sprintf("UPDATE %s SET %s WHERE %s", u.Table, columnsStr, conditionStr)

	return updateStr
}

func getOperation(data map[string]interface{}) (string, error) {

	op, exists := data["o"].(map[string]interface{})["diff"]
	if !exists {
		return "", errors.New("no operation found")
	}

	var operation []string

	for key, _ := range op.(map[string]interface{}) {
		operation = append(operation, key)
	}

	if len(operation) == 0 {
		return "", errors.New("no operation found")
	}

	switch operation[0] {
	case "u":
		return "u", nil
	case "d":
		return "d", nil
	default:
		return "", errors.New("unknown operation")
	}

}

func (u *Update) getColumns(data map[string]interface{}, operation string) []KeyValue {

	var result []KeyValue

	object, ok := data["o"].(map[string]interface{})["diff"].(map[string]interface{})[operation]

	if !ok {
		return result
	}

	for key, value := range object.(map[string]interface{}) {
		data := KeyValue{Key: key, Value: value}
		result = append(result, data)
	}

	return result
}

func (u *Update) getCondition(data map[string]interface{}) (KeyValue, error) {
	condition, exists := data["o2"].(map[string]interface{})
	if !exists {
		return KeyValue{}, errors.New("no condition found")
	}

	var result []KeyValue

	for key, value := range condition {
		result = append(result, KeyValue{Key: key, Value: value})
	}
	if len(result) == 0 {
		return KeyValue{}, errors.New("no condition found")
	}
	return result[0], nil
}
