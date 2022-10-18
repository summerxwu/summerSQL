package logicplan

import (
	"fmt"
	"summerSQL/catalog"
	"testing"
)

type planNodeForTest struct {
	body       string
	ChildNodes []ILogicPlan
}

func (p planNodeForTest) Schema() *catalog.Schema {
	// TODO implement me
	panic("implement me")
}

func (p planNodeForTest) Children() []ILogicPlan {
	// TODO implement me
	return p.ChildNodes
}

func (p planNodeForTest) ToString() string {
	// TODO implement me
	return p.body
}

func newTestPlans() ILogicPlan {
	scan := planNodeForTest{body: "Scan: employee.csv; projection=None"}
	scan1 := planNodeForTest{body: "Scan: employee.csv; projection=None"}
	filter := planNodeForTest{body: "Filter: #state = 'CO'", ChildNodes: make([]ILogicPlan, 0)}
	filter.ChildNodes = append(filter.ChildNodes, &scan)
	filter.ChildNodes = append(filter.ChildNodes, &scan1)
	projection := planNodeForTest{
		body: "Projection: #id, #first_name, #last_name, #state, #salary", ChildNodes: make([]ILogicPlan, 0),
	}
	projection.ChildNodes = append(projection.ChildNodes, &filter)
	return &projection
}

func TestPrintPretty(t *testing.T) {
	plan := newTestPlans()
	fmt.Println(PrintPretty(plan, "", "   "))
}
