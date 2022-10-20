package logical_plan

import (
	"summerSQL/catalog"
	"summerSQL/datasource"
)

// ExecutionContext create a valid datasource and return a dataframe
// only support CSV datasource for now
type ExecutionContext struct {
}

func (e ExecutionContext) CSVDataFrame(path string, schema catalog.Schema) (*DataFrame, error) {
	ds := datasource.NewCSVDataSource(path, schema, 100)
	scan, err := NewScan(ds, nil)
	if err != nil {
		return nil, err
	}
	df := DataFrame{logicPlan: scan}
	return &df, nil
}
