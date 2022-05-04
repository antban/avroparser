package schema

import (
	"fmt"
	"io"
	"reflect"
)

type lowOverheadReader struct {
	r io.Reader
}

func (lor lowOverheadReader) ReadByte() (byte, error) {
	result := []byte{0}
	if count, err := lor.r.Read(result); err != nil {
		return result[0], err
	} else if count != 1 {
		return result[0], fmt.Errorf("failed to read 1 byte, instead read %d bytes", count)
	} else {
		return result[0], nil
	}
}

func getStringValue(items map[string]interface{}, name string, required bool) (string, error) {
	value, exists := items[name]
	if required && !exists {
		return "", fmt.Errorf("required field %s not found in %v", name, items)
	}
	if !exists {
		return "", nil
	}
	if stringValue, ok := value.(string); !ok {
		return "", fmt.Errorf("field %s expected to be of type string in %v", name, items)
	} else {
		return stringValue, nil
	}
}

func getIntValue(items map[string]interface{}, name string, required bool) (int, error) {
	value, exists := items[name]
	if required && !exists {
		return 0, fmt.Errorf("required field %s not found in %v", name, items)
	}
	if !exists {
		return 0, nil
	}
	if intValue, ok := value.(int); !ok {
		return 0, fmt.Errorf("field %s expected to be of type string in %v", name, items)
	} else {
		return intValue, nil
	}
}

func readStringArray(data map[string]interface{}, name string, required bool) ([]string, error) {
	items, exists := data[name]
	if !exists {
		if required {
			return nil, fmt.Errorf("values %s are required in %v", name, data)
		} else {
			return []string{}, nil
		}
	}

	if kind := reflect.TypeOf(items).Kind(); kind != reflect.Array && kind != reflect.Slice {
		return nil, fmt.Errorf("%s should be an array in type %v", name, data)
	}

	resultValues := make([]string, 0)
	for _, item := range items.([]interface{}) {
		if convertedItem, conversionOk := item.(string); !conversionOk {
			return nil, fmt.Errorf("%s should be string in type %v", name, data)
		} else {
			resultValues = append(resultValues, convertedItem)
		}
	}
	return resultValues, nil
}
