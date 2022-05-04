package schema

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

type ItemSchema interface {
	Read(reader io.Reader) (interface{}, error)
}

///////////////////////

type AvroNull struct {
}

func (v AvroNull) Read(_ io.Reader) (interface{}, error) {
	return nil, nil
}

///////////////////////

type AvroBoolean struct {
}

func (v AvroBoolean) Read(r io.Reader) (interface{}, error) {
	rr := lowOverheadReader{r: r}
	if item, err := rr.ReadByte(); err != nil {
		return false, fmt.Errorf("failed to read boolean: %w", err)
	} else {
		if item == 0 {
			return false, nil
		} else {
			return true, nil
		}
	}
}

///////////////////////

type AvroInt struct {
}

func readInt(r io.Reader) (int32, error) {
	rr := lowOverheadReader{r: r}
	if result, err := binary.ReadVarint(rr); err != nil {
		return 0, fmt.Errorf("failed to read value: %w", err)
	} else if result >= math.MaxUint32 || result < 0 {
		return 0, fmt.Errorf("number %d is out of range for int32", result)
	} else {
		int32Val := int32((result >> 1) ^ (-(result & 1)))
		return int32Val, nil
	}
}

func (v AvroInt) Read(r io.Reader) (interface{}, error) {
	return readInt(r)
}

///////////////////////

type AvroLong struct {
}

func readLong(r io.Reader) (int64, error) {
	rr := lowOverheadReader{r: r}
	if result, err := binary.ReadUvarint(rr); err != nil {
		return 0, fmt.Errorf("failed to read value: %w", err)
	} else if result >= math.MaxUint64 || result < 0 {
		return 0, fmt.Errorf("number %d is out of range for int32", result)
	} else {
		int64Val := int64((result >> 1) ^ (-(result & 1)))
		return int64Val, nil
	}
}

func (v AvroLong) Read(r io.Reader) (interface{}, error) {
	return readLong(r)
}

///////////////////////

type AvroFloat struct {
}

func (v AvroFloat) Read(r io.Reader) (interface{}, error) {
	data := uint32(0)
	if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
		return float32(0), err
	} else {
		return math.Float32frombits(data), nil
	}
}

///////////////////////

type AvroDouble struct {
}

func (v AvroDouble) Read(r io.Reader) (interface{}, error) {
	data := uint64(0)
	if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
		return float32(0), err
	} else {
		return math.Float64frombits(data), nil
	}
}

///////////////////////

type AvroBytes struct {
}

func (v AvroBytes) Read(r io.Reader) (interface{}, error) {
	length, err := readLong(r)
	if nil != err {
		return nil, err
	}
	result := make([]byte, length)
	countRead, err := r.Read(result)
	if nil != err {
		return nil, fmt.Errorf("failed to read bytes contents: %w", err)
	} else if int64(countRead) != length {
		return nil, fmt.Errorf("not enough bytes (%d) to read contents (%d)", countRead, length)
	}
	return result, nil
}

///////////////////////

type AvroString struct {
}

func (v AvroString) Read(r io.Reader) (interface{}, error) {
	return readString(r)
}

func readString(r io.Reader) (string, error) {
	length, err := readLong(r)
	if nil != err {
		return "", err
	}
	result := make([]byte, length)
	countRead, err := r.Read(result)
	if nil != err {
		return "", fmt.Errorf("failed to read bytes contents: %w", err)
	} else if int64(countRead) != length {
		return "", fmt.Errorf("not enough bytes (%d) to read contents (%d)", countRead, length)
	}
	return string(result), nil
}

///////////////////////

type AvroRecordFieldOrder string

const (
	AvroRecordFieldOrderAscending  = AvroRecordFieldOrder("ascending")
	AvroRecordFieldOrderDescending = AvroRecordFieldOrder("descending")
	AvroRecordFieldOrderIgnore     = AvroRecordFieldOrder("ignore")
)

type AvroRecordField struct {
	name         string
	doc          string
	fieldType    ItemSchema
	defaultValue interface{}
	order        AvroRecordFieldOrder
	aliases      []string
}

type AvroRecord struct {
	name      string
	namespace string
	doc       string
	aliases   []string
	fields    []AvroRecordField
}

func (v AvroRecord) Read(r io.Reader) (interface{}, error) {
	result := make(map[string]interface{})
	for _, f := range v.fields {
		if value, err := f.fieldType.Read(r); err != nil {
			return result, fmt.Errorf("failed reading %s in type %s: %w", f.name, v.name, err)
		} else {
			result[f.name] = value
			//for _, alias := range f.aliases {
			//	result[alias] = value
			//}
		}
	}
	return result, nil
}

///////////////////////

type AvroEnum struct {
	name         string
	namespace    string
	aliases      []string
	doc          string
	symbols      []string
	defaultValue *string
}

func (v AvroEnum) Read(r io.Reader) (interface{}, error) {
	value, err := readInt(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s enum value: %w", v.name, err)
	}
	if value >= int32(len(v.symbols)) || value < 0 {
		if nil != v.defaultValue && value >= 0 {
			return v.defaultValue, nil
		} else {
			return nil, fmt.Errorf("no enum constant defined for %d, enum %s", value, v.name)
		}
	} else {
		return v.symbols[value], nil
	}
}

///////////////////////

type AvroArray struct {
	itemSchema ItemSchema
}

func (v AvroArray) Read(r io.Reader) (interface{}, error) {
	hasRecords := true
	var result []interface{} = nil
	for hasRecords {
		count, err := readLong(r)
		if err != nil {
			return nil, fmt.Errorf("failed to read array length: %w", err)
		}
		if count == 0 {
			hasRecords = false
		} else if count < 0 {
			count = -count
			// fast skip is used, but we are not fast skipping
			_, err = readLong(r)
			if err != nil {
				return nil, fmt.Errorf("failed to read array fast skip section %w", err)
			}
		}
		if result == nil {
			result = make([]interface{}, count)
		}
		for idx := 0; idx < int(count); idx++ {
			result[idx], err = v.itemSchema.Read(r)
			if err != nil {
				return nil, fmt.Errorf("failed to read item at idx %d: %w", idx, err)
			}
		}
	}
	return result, nil
}

///////////////////////

type AvroFixed struct {
	name      string
	namespace string
	aliases   []string
	doc       string
	size      int
}

func (v AvroFixed) Read(r io.Reader) (interface{}, error) {
	result := make([]byte, v.size)

	if read, err := r.Read(result); err != nil {
		return 0, fmt.Errorf("failed to read fixed value: %w", err)
	} else if read != v.size {
		return 0, fmt.Errorf("number of fixed bytes read %d is not equal to expected: %d", read, v.size)
	} else {
		return result, nil
	}
}

///////////////////////

type AvroUnion struct {
	elements []ItemSchema
}

func (v AvroUnion) Read(r io.Reader) (interface{}, error) {
	if idx, err := readInt(r); err != nil {
		return nil, err
	} else if idx < 0 || int(idx) >= len(v.elements) {
		return nil, fmt.Errorf("union doesn't have element with index %d", idx)
	} else {
		return v.elements[idx].Read(r)
	}
}

///////////////////////

type AvroMap struct {
	values ItemSchema
}

func (v AvroMap) Read(r io.Reader) (interface{}, error) {
	hasRecords := true
	result := make(map[string]interface{})
	for hasRecords {
		count, err := readLong(r)
		if err != nil {
			return nil, fmt.Errorf("failed to read array length: %w", err)
		}
		if count == 0 {
			hasRecords = false
		} else if count < 0 {
			count = -count
			// fast skip is used, but we are not fast skipping
			_, err = readLong(r)
			if err != nil {
				return nil, fmt.Errorf("failed to read array fast skip section %w", err)
			}
		}
		for idx := 0; idx < int(count); idx++ {
			name, err := readString(r)
			if nil != err {
				return nil, fmt.Errorf("failed to read name in map %w", err)
			}
			result[name], err = v.values.Read(r)
			if err != nil {
				return nil, fmt.Errorf("failed to read item with name %s: %w", name, err)
			}
		}
	}
	return result, nil
}
