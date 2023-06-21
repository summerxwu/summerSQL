package logical_plan

import "summerSQL/catalog"

type DataFrame struct {
	Final_logicPlan ILogicPlan
}

func NewDataFrame() *DataFrame {
	return &DataFrame{Final_logicPlan: nil}
}

func (d *DataFrame) Projection(exprs []ILogicExpr) *DataFrame {
	if d.Final_logicPlan == nil {
		panic("missing input logic plan for projection")
	}
	input := make([]ILogicPlan, 0)
	input = append(input, d.Final_logicPlan)
	d.Final_logicPlan = NewProjector(input, exprs)
	return d
}

func (d *DataFrame) Filter(exprs ILogicExpr) *DataFrame {
	if d.Final_logicPlan == nil {
		panic("missing input logic plan for Filter")
	}
	input := make([]ILogicPlan, 0)
	input = append(input, d.Final_logicPlan)
	d.Final_logicPlan = NewFilter(input, exprs)
	return d
}

func (d *DataFrame) Aggregate(aggrExprs []ILogicExpr, groupExprs []ILogicExpr) *DataFrame {
	if d.Final_logicPlan == nil {
		panic("missing input logic plan for Aggregate")
	}
	d.Final_logicPlan = NewAggregate(d.Final_logicPlan, aggrExprs, groupExprs)
	return d
}

func (d *DataFrame) Schema() *catalog.Schema {
	return d.Final_logicPlan.Schema()
}

func (d *DataFrame) LogicPlan() ILogicPlan {
	return d.Final_logicPlan
}
