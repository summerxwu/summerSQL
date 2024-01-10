package logical_plan

/*
type planNodeForTest struct {
	body       string
	ChildNodes []ILogicOperator
}

func (p planNodeForTest) Schema() *catalog.TSchema {
	// TODO implement me
	panic("implement me")
}

func (p planNodeForTest) Children() []ILogicOperator {
	// TODO implement me
	return p.ChildNodes
}

func (p planNodeForTest) ToString() string {
	// TODO implement me
	return p.body
}

func newTestPlans() ILogicOperator {
	scan := planNodeForTest{body: "LogicalScan: employee.csv; projection=None"}
	scan1 := planNodeForTest{body: "LogicalScan: employee.csv; projection=None"}
	filter := planNodeForTest{body: "LogicalFilter: #state = 'CO'", ChildNodes: make([]ILogicOperator, 0)}
	filter.ChildNodes = append(filter.ChildNodes, &scan)
	filter.ChildNodes = append(filter.ChildNodes, &scan1)
	projection := planNodeForTest{
		body: "Projection: #id, #first_name, #last_name, #state, #salary", ChildNodes: make([]ILogicOperator, 0),
	}
	projection.ChildNodes = append(projection.ChildNodes, &filter)
	return &projection
}

func TestPrintPretty(t *testing.T) {
	plan := newTestPlans()
	fmt.Println(PrintPretty(plan, "", "   "))
}


*/
