package pipeflow

import (
	"fmt"
	"reflect"
	"time"
)

const (
	maxMetadataEntries = 64
	maxMetadataKeySize = 128
	maxMetadataString  = 4096
)

// ResultMetadata contains small scalar facts about successful execution. It is
// copied into reports and must not contain business payloads.
type ResultMetadata map[string]any

type compiledMetadataExtractor struct {
	extract   func(any) (ResultMetadata, error)
	inputType reflect.Type
}

var resultMetadataType = reflect.TypeOf(ResultMetadata{})

func compileMetadataExtractor(owner, name string, extractor any, outputType reflect.Type) (*compiledMetadataExtractor, error) {
	t := reflect.TypeOf(extractor)
	if t == nil || t.Kind() != reflect.Func || t.NumIn() > 1 || t.NumOut() != 1 || t.Out(0) != resultMetadataType {
		return nil, fmt.Errorf("pipeflow: %s %q result metadata extractor must be func() ResultMetadata or func(T) ResultMetadata", owner, name)
	}
	var inputType reflect.Type
	if t.NumIn() == 1 {
		inputType = t.In(0)
		if outputType != nil && !outputType.AssignableTo(inputType) {
			return nil, fmt.Errorf("pipeflow: %s %q result metadata extractor expects %s but result is %s", owner, name, inputType, outputType)
		}
	}
	v := reflect.ValueOf(extractor)
	return &compiledMetadataExtractor{
		inputType: inputType,
		extract: func(value any) (metadata ResultMetadata, err error) {
			defer recoverPanic(&err)
			args := make([]reflect.Value, 0, 1)
			if inputType != nil {
				if err := acceptsValue(inputType, value); err != nil {
					return nil, fmt.Errorf("result metadata input: %w", err)
				}
				args = append(args, reflectValue(value, inputType))
			}
			metadata = v.Call(args)[0].Interface().(ResultMetadata)
			return validateAndCloneMetadata(metadata)
		},
	}, nil
}

func validateAndCloneMetadata(metadata ResultMetadata) (ResultMetadata, error) {
	if metadata == nil {
		return nil, nil
	}
	if len(metadata) > maxMetadataEntries {
		return nil, fmt.Errorf("pipeflow: result metadata has %d entries; maximum is %d", len(metadata), maxMetadataEntries)
	}
	result := make(ResultMetadata, len(metadata))
	for key, value := range metadata {
		if key == "" {
			return nil, fmt.Errorf("pipeflow: result metadata key cannot be empty")
		}
		if len(key) > maxMetadataKeySize {
			return nil, fmt.Errorf("pipeflow: result metadata key %q exceeds %d bytes", key, maxMetadataKeySize)
		}
		if err := validateMetadataValue(key, value); err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}

func validateMetadataValue(key string, value any) error {
	if value == nil {
		return nil
	}
	if text, ok := value.(string); ok {
		if len(text) > maxMetadataString {
			return fmt.Errorf("pipeflow: result metadata value %q exceeds %d bytes", key, maxMetadataString)
		}
		return nil
	}
	if _, ok := value.(time.Time); ok {
		return nil
	}
	switch reflect.TypeOf(value).Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return nil
	default:
		return fmt.Errorf("pipeflow: result metadata value %q has unsupported type %T", key, value)
	}
}
