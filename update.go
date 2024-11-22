package chroma

import (
	"errors"
)

type Update struct {
	Op       string
	Database string
	Table    string
	Column   KeyValue
	Query    KeyValue
}

func NewUpdate() Update {

	return Update{}
}

func (u *Update) Parse(data map[string]interface{}) (*Update, error) {

	ns := getNamespace(data)

	match, err := extractNamespace(ns)

	if err != nil {
		return u, err
	}

	u.Database = match[1]
	u.Table = match[2]

	op, err := getOperation(data)
	if err != nil {
		return u, err
	}

	u.Op = op

	u.Column = u.getColumns(data, u.Op)[0]

	query, err := u.getQuery(data)

	if err != nil {
		return u, err
	}
	u.Query = query

	return u, nil
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

func (u *Update) getQuery(data map[string]interface{}) (KeyValue, error) {
	query, exists := data["o2"].(map[string]interface{})
	if !exists {
		return KeyValue{}, errors.New("no query found")
	}

	var result []KeyValue

	for key, value := range query {
		result = append(result, KeyValue{Key: key, Value: value})
	}
	if len(result) == 0 {
		return KeyValue{}, errors.New("no query found")
	}
	return result[0], nil
}
