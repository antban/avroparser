package schema

import (
	"fmt"
	"io"
	"reflect"
)

type avroReferenceSchema struct {
	name string
	ref  ItemSchema
}

func (v avroReferenceSchema) Read(reader io.Reader) (interface{}, error) {
	return v.ref.Read(reader)
}

type schemaBuilder struct {
	references   []avroReferenceSchema
	namedSchemas map[string]ItemSchema
}

func (builder *schemaBuilder) read(schema interface{}) (ItemSchema, error) {
	// step 1. read all schema elements
	root, err := builder.readSchemaElement(schema)
	if err != nil {
		return nil, err
	}
	// step 2. resolve reference to schema elements
	for _, reference := range builder.references {
		if actualSchema, found := builder.namedSchemas[reference.name]; found {
			reference.ref = actualSchema
		} else {
			return nil, fmt.Errorf("failed to find reference to schema with name %s", reference.name)
		}
	}
	return root, nil
}

func (builder *schemaBuilder) readUnion(schemaItems []interface{}) (ItemSchema, error) {
	schemas := make([]ItemSchema, len(schemaItems))
	for idx, elem := range schemaItems {
		if value, err := builder.readSchemaElement(elem); err != nil {
			return nil, err
		} else {
			schemas[idx] = value
		}
	}
	return AvroUnion{elements: schemas}, nil
}

func (builder *schemaBuilder) readRecordField(data map[string]interface{}) (AvroRecordField, error) {
	result := AvroRecordField{}
	var err error
	if result.name, err = getStringValue(data, "name", true); err != nil {
		return result, err
	}
	if result.doc, err = getStringValue(data, "doc", false); err != nil {
		return result, err
	}
	if typeValue, found := data["type"]; !found {
		return result, fmt.Errorf("type is not found in avro field %v", data)
	} else {
		if result.fieldType, err = builder.readSchemaElement(typeValue); err != nil {
			return result, err
		}
	}
	result.defaultValue, _ = data["default"]
	if result.aliases, err = readStringArray(data, "aliases", false); err != nil {
		return result, err
	}
	return result, nil
}

func (builder *schemaBuilder) readRecord(data map[string]interface{}) (ItemSchema, error) {
	result := AvroRecord{
		aliases: make([]string, 0),
		fields:  make([]AvroRecordField, 0),
	}
	var err error
	if result.name, err = getStringValue(data, "name", true); err != nil {
		return nil, err
	}
	if result.namespace, err = getStringValue(data, "namespace", false); err != nil {
		return nil, err
	}
	if result.doc, err = getStringValue(data, "doc", false); err != nil {
		return nil, err
	}
	if result.aliases, err = readStringArray(data, "aliases", false); err != nil {
		return nil, err
	}
	if fields, exists := data["fields"]; exists {
		if kind := reflect.TypeOf(fields).Kind(); kind != reflect.Array && kind != reflect.Slice {
			return nil, fmt.Errorf("fields should be an array in type %v", result)
		}
		for _, fieldData := range fields.([]interface{}) {
			if fieldDataMap, ok := fieldData.(map[string]interface{}); !ok {
				return nil, fmt.Errorf("field data should be a map in %v", data)
			} else if fieldObject, err := builder.readRecordField(fieldDataMap); err != nil {
				return nil, err
			} else {
				result.fields = append(result.fields, fieldObject)
			}
		}
	}
	builder.namedSchemas[result.name] = result
	return result, err
}

func (builder *schemaBuilder) readEnum(data map[string]interface{}) (ItemSchema, error) {
	result := AvroEnum{}
	var err error
	if result.name, err = getStringValue(data, "name", true); err != nil {
		return result, err
	}
	if result.namespace, err = getStringValue(data, "namespace", false); err != nil {
		return nil, err
	}
	if result.doc, err = getStringValue(data, "doc", false); err != nil {
		return nil, err
	}
	if result.aliases, err = readStringArray(data, "aliases", false); err != nil {
		return nil, err
	}
	if defaultValue, defaultValuePresent := data["default"]; defaultValuePresent {
		if defaultValueString, ok := defaultValue.(string); !ok {
			return nil, fmt.Errorf("default value should be string")
		} else {
			result.defaultValue = &defaultValueString
		}
	}
	if result.symbols, err = readStringArray(data, "symbols", true); err != nil {
		return result, err
	}
	builder.namedSchemas[result.name] = result
	return result, nil
}

func (builder *schemaBuilder) readArray(data map[string]interface{}) (ItemSchema, error) {
	result := AvroArray{}
	var err error
	if schema, exists := data["items"]; !exists {
		return result, fmt.Errorf("items schema is not set for array %v", data)
	} else if result.itemSchema, err = builder.readSchemaElement(schema); err != nil {
		return result, err
	} else {
		return result, nil
	}
}

func (builder *schemaBuilder) readMap(data map[string]interface{}) (ItemSchema, error) {
	result := AvroMap{}
	var err error
	if schema, exists := data["values"]; !exists {
		return result, fmt.Errorf("values schema is not set for map %v", data)
	} else if result.values, err = builder.readSchemaElement(schema); err != nil {
		return result, err
	} else {
		return result, nil
	}
}

func (builder *schemaBuilder) readFixed(data map[string]interface{}) (ItemSchema, error) {
	result := AvroFixed{}
	var err error
	if result.name, err = getStringValue(data, "name", true); err != nil {
		return result, err
	}
	if result.namespace, err = getStringValue(data, "namespace", false); err != nil {
		return nil, err
	}
	if result.doc, err = getStringValue(data, "doc", false); err != nil {
		return nil, err
	}
	if result.aliases, err = readStringArray(data, "aliases", false); err != nil {
		return nil, err
	}
	if result.size, err = getIntValue(data, "size", true); err != nil {
		return nil, err
	}
	builder.namedSchemas[result.name] = result
	return result, nil
}

func (builder *schemaBuilder) readSchemaElement(schema interface{}) (ItemSchema, error) {
	var typeName string
	var typeData map[string]interface{}

	schemaType := reflect.TypeOf(schema)
	if schemaType.Kind() == reflect.Array || schemaType.Kind() == reflect.Slice {
		return builder.readUnion(schema.([]interface{}))
	} else if schemaType.Kind() == reflect.String {
		typeName = schema.(string)
		typeData = nil
	} else if schemaType.Kind() == reflect.Map {
		m := schema.(map[string]interface{})
		if name, present := m["type"]; !present {
			return nil, fmt.Errorf("failed to parse type of schema from %v", m)
		} else {
			if nameString, ok := name.(string); !ok {
				return nil, fmt.Errorf("type name is expected to be string in %v", schema)
			} else {
				typeName = nameString
				typeData = m
			}
		}
	} else {
		return nil, fmt.Errorf("kind of element is not supported to recover schema: %v", schema)
	}

	switch typeName {
	case "null":
		return AvroNull{}, nil
	case "boolean":
		return AvroBoolean{}, nil
	case "int":
		return AvroInt{}, nil
	case "long":
		return AvroLong{}, nil
	case "float":
		return AvroFloat{}, nil
	case "double":
		return AvroDouble{}, nil
	case "bytes":
		return AvroBytes{}, nil
	case "string":
		return AvroString{}, nil
	case "record":
		return builder.readRecord(typeData)
	case "enum":
		return builder.readEnum(typeData)
	case "array":
		return builder.readArray(typeData)
	case "map":
		return builder.readMap(typeData)
	case "fixed":
		return builder.readFixed(typeData)
	default:
		fake := avroReferenceSchema{name: typeName}
		builder.references = append(builder.references, fake)
		return fake, nil
	}
}

func ParseSchema(schema interface{}) (ItemSchema, error) {
	builder := schemaBuilder{
		references:   make([]avroReferenceSchema, 0),
		namedSchemas: make(map[string]ItemSchema),
	}
	return builder.read(schema)
}
