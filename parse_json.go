package chroma

import (
	"encoding/json"
	"fmt"
)

type Oplog struct {
	Op        string      `json:"op"`
	Namespace string      `json:"ns"`
	Object    interface{} `json:"o"`
}

func ParseJSON(oplog []byte) (Oplog, error) {
	var result Oplog
	err := json.Unmarshal(oplog, &result)

	if err != nil {
		return result, fmt.Errorf("error parsing oplog as JSON: %w", err)
	}

	if result.Op == "i" {
		result.Op = "insert"
	}

	return result, nil
}
