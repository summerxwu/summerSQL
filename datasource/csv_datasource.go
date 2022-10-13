package datasource

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"summerSQL/catalog"
)

type CSVDataSource struct {
	FileName     string
	csvSchema    catalog.Schema
	readerSchema catalog.Schema
	csvReader    *csv.Reader
	arrayBuilder map[string]*catalog.ColumnVectorBuilder
	LastOffset   int
	BatchSize    int
}

func NewCSVDataSource(filename string, schema catalog.Schema, batchSize int) *CSVDataSource {
	csvDataSource := CSVDataSource{
		FileName:     filename,
		csvSchema:    schema,
		readerSchema: catalog.Schema{Fields: make([]*catalog.Column, 0)},
		csvReader:    nil,
		arrayBuilder: make(map[string]*catalog.ColumnVectorBuilder, 0),
		LastOffset:   0,
		BatchSize:    batchSize,
	}
	if len(schema.Fields) == 0 {
		return nil
	}
	for _, column := range schema.Fields {
		builder := catalog.NewArrowColumnVectorBuilder(column)
		if builder != nil {
			csvDataSource.arrayBuilder[column.Name] = builder
		} else {
			return nil
		}
	}
	rt, err := os.Open(csvDataSource.FileName)
	if err != nil {
		return nil
	}
	csvDataSource.csvReader = csv.NewReader(rt)
	return &csvDataSource
}

func (C *CSVDataSource) Schema() catalog.Schema {
	return C.csvSchema
}

func (C *CSVDataSource) Scan(projections catalog.Schema) (catalog.BatchColumns, error) {
	return C.createAndFillBatch(projections)
}

func (C *CSVDataSource) createBatchFromBuilders() catalog.BatchColumns {

	bColumns := catalog.BatchColumns{
		BatchSchema: C.readerSchema,
		BatchVector: make([]catalog.IColumnVector, 0),
	}

	for _, field := range bColumns.BatchSchema.Fields {
		builder := C.arrayBuilder[field.Name].Builder

		cv := catalog.NewArrowColumnVector(field)
		cv.Value = builder.NewArray()
		cv.Length = cv.Value.Len()

		bColumns.BatchVector = append(bColumns.BatchVector, cv)
	}
	return bColumns
}

func (C *CSVDataSource) dealRawDataByBuilders(rawData []string) error {
	for _, i2 := range C.readerSchema.Fields {
		builder := C.arrayBuilder[i2.Name]
		index := i2.Index
		err := builder.Append(rawData[index])
		if err != nil {
			return err
		}
	}

	return nil
}

func (C *CSVDataSource) createAndFillBatch(projections catalog.Schema) (catalog.BatchColumns, error) {
	var err error
	C.readerSchema = projections

	batchCounter := C.BatchSize
	if batchCounter <= 0 {
		err = errors.New("datasource batch size is not positive")
		goto error
	}

	for {
		rawRecord, err := C.csvReader.Read()

		if err == io.EOF || batchCounter <= 0 {
			return C.createBatchFromBuilders(), nil
		}
		err = C.dealRawDataByBuilders(rawRecord)
		if err != nil {
			goto error
		}
		batchCounter--
	}

error:
	return catalog.BatchColumns{}, err

}
