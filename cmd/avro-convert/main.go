package main

import (
	"avroparser/pkg/provider"
	"encoding/json"
	"flag"
	"io"
	"os"
)

func main() {
	staticSchema := flag.String("s", "", "path to file with avro schema for source data")
	flag.Parse()

	var streamConverter provider.StreamConverter
	var err error

	if *staticSchema != "" {
		streamConverter, err = provider.NewStaticFileStreamConverter(*staticSchema)
		if err != nil {
			panic(err)
		}
	} else {
		panic("stream converter / schema provider is not set")
	}

	var input io.Reader
	input = os.Stdin

	var output io.Writer
	output = os.Stdout

	hasData := true
	for hasData {
		wrapper := provider.NewCountingReader(input)
		data, err := streamConverter.Next(&wrapper)
		if nil != err {
			if provider.IsEof(err) && wrapper.ReadBytes() == 0 {
				hasData = false
			} else {
				panic(err)
			}
		} else {
			displayData(data, output)
		}
	}
}

func displayData(data []provider.DataChunk, output io.Writer) {
	if len(data) == 0 {
		return
	}
	var toDisplay interface{}
	if len(data) == 1 && data[0].Name() == "" {
		toDisplay = data[0].Value()
	} else {
		mp := make(map[string]interface{})
		for _, ch := range data {
			mp[ch.Name()] = ch.Value()
		}
		toDisplay = mp
	}
	dataBytes, err := json.Marshal(toDisplay)
	if err != nil {
		panic(err)
	}
	_, err = output.Write(dataBytes)
	if err != nil {
		panic(err)
	}
}
