package domain

import (
	"encoding/json"
	"errors"
	"fmt"
)

const MaxSagaContextBytes = 65536 // 64 KiB

var ErrInvalidSagaContext = errors.New("invalid saga context")

func ParseSagaContext(rawContext []byte) (map[string]any, error) {
	if len(rawContext) == 0 {
		return nil, ErrInvalidSagaContext
	}

	if len(rawContext) > MaxSagaContextBytes {
		return nil, fmt.Errorf("%w: context exceeds %d bytes", ErrInvalidSagaContext, MaxSagaContextBytes)
	}

	var parsed map[string]any
	if err := json.Unmarshal(rawContext, &parsed); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSagaContext, err)
	}

	if parsed == nil {
		return nil, ErrInvalidSagaContext
	}

	return parsed, nil
}
