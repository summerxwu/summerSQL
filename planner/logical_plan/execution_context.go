package logical_plan

/*
// ExecutionContext create a valid access and return a dataframe
// only support CSV access for now
type ExecutionContext struct {
}

func (e ExecutionContext) CSVDataFrame(path string, schema catalog.TSchema) (*DataFrame, error) {
	_, err := access.NewInferringCSVDataSource(path, 100)
	scan, err := NewScan("test")
	if err != nil {
		return nil, err
	}
	df := DataFrame{Final_logicPlan: scan}
	return &df, nil
}


*/
