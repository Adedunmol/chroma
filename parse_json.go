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

	switch dest["op"] {
	case "i":
		dest["op"] = "insert"
		break
	case "u":
		dest["op"] = "update"
		break
	default:
		return map[string]interface{}{}, fmt.Errorf("%w: %s", UnknownOp, dest["op"])
	}

	return dest, nil
}
