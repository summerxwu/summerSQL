package access

import "C"
import (
	"github.com/apache/arrow/go/v11/arrow/csv"
	"os"
	"summerSQL/catalog"
	"summerSQL/util"
)

type CSVDataSource struct {
	FileName   string
	csvSchema  *catalog.TSchema
	csvReader  *csv.Reader
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
	csvDataSource := CSVDataSource{
		FileName:   fileName,
		csvSchema:  nil,
		csvReader:  csv.NewInferringReader(rt, csv.WithChunk(1)),
		LastOffset: 0,
		BatchSize:  batchSize,
	}
	return &csvDataSource, nil
}

func NewCSVDataSource(fileName string, schema *catalog.TSchema, batchSize int) (*CSVDataSource, error) {
	rt, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &CSVDataSource{
			FileName:   fileName,
			csvSchema:  schema,
			csvReader:  csv.NewReader(rt, schema, csv.WithChunk(1), csv.WithHeader(true)),
			LastOffset: 0,
			BatchSize:  batchSize},
		nil
}

func (d *CSVDataSource) Schema() *catalog.TSchema {
	return d.csvSchema
}

func (d *CSVDataSource) Inhale() (catalog.IBatch, error) {
	if d.BatchSize <= 0 {
		return nil, util.NewErrorfWithCode(util.BatchSizeExhausted, "Batch size exhausted")
	}
	if !d.csvReader.Next() {
		return nil, d.csvReader.Err()
	}
	err := d.csvReader.Err()
	if err != nil {
		return nil, err
	}
	d.BatchSize--
	return d.csvReader.Record(), nil
}
