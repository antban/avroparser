package provider

import (
	"errors"
	"io"
)

type CountingReader struct {
	bytesRead int
	reader    io.Reader
}

func NewCountingReader(reader io.Reader) CountingReader {
	return CountingReader{
		bytesRead: 0,
		reader:    reader,
	}
}

func (r *CountingReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if err != nil {
		r.bytesRead += n
	}
	return
}

func (r *CountingReader) ReadBytes() int {
	return r.bytesRead
}

func IsEof(err error) bool {
	if err == io.EOF {
		return true
	}
	unwrapped := errors.Unwrap(err)
	if nil != unwrapped {
		if unwrapped == err {
			// some people like recursion
			return false
		}
		return IsEof(unwrapped)
	}
	return false
}
