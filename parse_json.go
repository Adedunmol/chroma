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
