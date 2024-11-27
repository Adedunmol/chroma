package chroma

import (
	"encoding/json"
	"errors"
	"fmt"
)

var UnknownOp = errors.New("unknown op")

type Oplog struct {
	Op        string                 `json:"op"`
	Namespace string                 `json:"ns"`
	Object    map[string]interface{} `json:"o"`
}

func ParseJSON(oplog []byte) (Oplog, error) {
	var result Oplog
	err := json.Unmarshal(oplog, &result)

	if err != nil {
		return result, fmt.Errorf("error parsing oplog as JSON: %w", err)
	}

	switch result.Op {
	case "i":
		result.Op = "insert"
		break
	case "u":
		result.Op = "update"
		break
	default:
		return result, fmt.Errorf("%w: %s", UnknownOp, result.Op)
	}

	return result, nil
}

func ParseJSONMap(oplog []byte) (map[string]interface{}, error) {
	var dest map[string]interface{}
	err := json.Unmarshal(oplog, &dest)

	if err != nil {
		return map[string]interface{}{}, fmt.Errorf("error parsing oplog as JSON: %w", err)
	}

	if len(dest) < 3 {
		return map[string]interface{}{}, fmt.Errorf("wrong structure")
	}

	err = validateOperation(dest)

	if err != nil {
		return map[string]interface{}{}, fmt.Errorf("error validating oplog as JSON: %w", err)
	}

	return dest, nil
}

func ParseJSONArray(oplog []byte) ([]map[string]interface{}, error) {
	var dest []map[string]interface{}
	err := json.Unmarshal(oplog, &dest)

	if err != nil {
		return []map[string]interface{}{}, fmt.Errorf("error parsing oplog as JSON: %w", err)
	}

	for _, oplog := range dest {
		err = validateOperation(oplog)
		if err != nil {
			return []map[string]interface{}{}, fmt.Errorf("error validating oplog as JSON: %w for %v", err, oplog)
		}
	}

	return dest, nil
}

func validateOperation(oplog map[string]interface{}) error {

	switch oplog["op"] {
	case "i":
		oplog["op"] = "insert"
		break
	case "u":
		oplog["op"] = "update"
		break
	case "d":
		oplog["op"] = "delete"
		break
	default:
		return fmt.Errorf("%w: %s", UnknownOp, oplog["op"])
	}

	return nil
}
