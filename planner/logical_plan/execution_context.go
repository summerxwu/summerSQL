package logical_plan

import (
	"summerSQL/access"
	"summerSQL/catalog"
)

// ExecutionContext create a valid access and return a dataframe
// only support CSV access for now
type ExecutionContext struct {
}

func (e ExecutionContext) CSVDataFrame(path string, schema catalog.Schema) (*DataFrame, error) {
	ds, err := access.NewInferringCSVDataSource(path, 100)
	scan, err := NewScan(ds.Schema(), "test")
	if err != nil {
		return nil, err
	}
	df := DataFrame{Final_logicPlan: scan}
	return &df, nil
}
