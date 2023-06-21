package access

import (
	"errors"
	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/csv"
	"os"
	"summerSQL/catalog"
)

type CSVDataSource struct {
	FileName   string
	csvSchema  catalog.Schema
	csvReader  array.RecordReader
	LastOffset int
	BatchSize  int
}

// NewInferringCSVDataSource will create a new datasource inhale from the CSV data
// file on disk and inferring the schema of the data.
func NewInferringCSVDataSource(fileName string, batchSize int) (*CSVDataSource, error) {

	rt, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	reader := csv.NewInferringReader(rt, csv.WithChunk(batchSize))
	csvDataSource := CSVDataSource{
		FileName:   fileName,
		csvSchema:  arrow.Schema{},
		csvReader:  reader,
		LastOffset: 0,
		BatchSize:  batchSize,
	}
	return &csvDataSource, nil
}

func (C *CSVDataSource) Schema() *catalog.Schema {
	return &C.csvSchema
}

func (C *CSVDataSource) Inhale() (catalog.IBatch, error) {

	reader, ok := C.csvReader.(*csv.Reader)
	if !ok {
		return nil, errors.New("type assertion failed: not a CSV file reader")
	}
	if !reader.Next() {
		return nil, reader.Err()
	}
	err := reader.Err()
	if err != nil {
		return nil, err
	}

	return reader.Record(), nil
}
