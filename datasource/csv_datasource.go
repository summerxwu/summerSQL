package datasource

import (
	"awesomeProject/catalog"
	"encoding/csv"
	"os"
)

type CSVDataSource struct {
	FileName     string
	csvSchema    catalog.Schema
	csvReader    *csv.Reader
	arrayBuilder map[string]*catalog.ColumnVectorBuilder
}

func NewCSVDataSource(filename string, schema catalog.Schema) *CSVDataSource {
	csvDataSource := CSVDataSource{
		csvSchema: schema, FileName: filename,
		arrayBuilder: make(map[string]*catalog.ColumnVectorBuilder, 0),
	}
	if len(schema.Fields) == 0 {
		return nil
	}
	for _, column := range schema.Fields {
		builder := catalog.NewArrowColumnVectorBuiler(column)
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

func (C *CSVDataSource) Scan(projections catalog.Schema) catalog.BatchColumns {
	batch := catalog.BatchColumns{
		BatchSchema: projections, BatchVector: make([]catalog.IColumnVector, 0),
	}
	for _, column := range projections.Fields {
		arrowCV := catalog.ArrowColumnVector{ColumnSpec: *column, Length: 0}
		builder := C.arrayBuilder[column.Name]
		arrowCV.Value = builder.Builder.NewArray()
		batch.BatchVector = append(batch.BatchVector, arrowCV)
	}
	return batch
}

func (C *CSVDataSource) createBatchColumns(projections catalog.Schema) catalog.BatchColumns {
	// TODO
	return catalog.BatchColumns{BatchSchema: projections}
}
