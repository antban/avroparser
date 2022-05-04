package provider

import (
	"avroparser/pkg/schema"
	"io"
)

type DataChunk struct {
	name   string
	schema schema.ItemSchema
	data   interface{}
}

func (ch DataChunk) Name() string {
	return ch.name
}

func (ch DataChunk) Value() interface{} {
	return ch.data
}

func NewDataChunk(name string, s schema.ItemSchema, r io.Reader) (DataChunk, error) {
	data, err := s.Read(r)
	if err != nil {
		return DataChunk{}, err
	}
	return DataChunk{name: name, schema: s, data: data}, nil
}
