package logical_plan

import "summerSQL/catalog"

type DataFrame struct {
	logicPlan ILogicPlan
}

func NewDataFrame() *DataFrame {
	return &DataFrame{logicPlan: nil}
}

func (d *DataFrame) Projection(exprs []ILogicExpr) *DataFrame {
	if d.logicPlan == nil {
		panic("missing input logic plan for projection")
	}
	d.logicPlan = NewProjection(d.logicPlan, exprs)

	return d
}

func (d *DataFrame) Filter(exprs []ILogicExpr) *DataFrame {
	if d.logicPlan == nil {
		panic("missing input logic plan for Filter")
	}
	d.logicPlan = NewFilter(d.logicPlan, exprs)
	return d
}

func (d *DataFrame) Aggregate(aggrExprs []ILogicExpr, groupExprs []ILogicExpr) *DataFrame {
	if d.logicPlan == nil {
		panic("missing input logic plan for Aggregate")
	}
	d.logicPlan = NewAggregate(d.logicPlan, aggrExprs, groupExprs)
	return d
}

func (d *DataFrame) Schema() *catalog.Schema {
	return d.logicPlan.Schema()
}

func (d *DataFrame) LogicPlan() ILogicPlan {
	return d.logicPlan
}
