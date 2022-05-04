package provider

import (
	"avroparser/pkg/schema"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
)

type Counting interface {
	BytesRead() uint64
}

type StreamConverter interface {
	Next(reader io.Reader) ([]DataChunk, error)
}

type StaticFileSchema struct {
	schema schema.ItemSchema
}

func NewStaticFileStreamConverter(fileName string) (*StaticFileSchema, error) {
	schemaData, err := ioutil.ReadFile(fileName)
	if nil != err {
		return nil, err
	}
	jsonSchema := make(map[string]interface{})
	err = json.Unmarshal(schemaData, &jsonSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse json %w", err)
	}
	parsedSchema, err := schema.ParseSchema(jsonSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema %w", err)
	}
	return &StaticFileSchema{schema: parsedSchema}, nil
}

func (sfs *StaticFileSchema) Next(reader io.Reader) ([]DataChunk, error) {
	chunk, err := NewDataChunk("", sfs.schema, reader)
	if nil == err {
		return []DataChunk{chunk}, nil
	}
	return nil, err
}
