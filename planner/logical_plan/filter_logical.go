package logical_plan

import (
	"bytes"
	"summerSQL/catalog"
)

type Filter struct {
	ChildPlans []ILogicPlan
	Expr       ILogicExpr
}

func NewFilter(childPlans []ILogicPlan, expr ILogicExpr) *Filter {
	return &Filter{ChildPlans: childPlans, Expr: expr}
}

func (f *Filter) Schema() *catalog.TSchema {
	return f.ChildPlans[0].Schema()
}

func (f *Filter) Children() []ILogicPlan {
	return f.ChildPlans
}

func (f *Filter) ToString() string {
	buff := bytes.Buffer{}
	buff.WriteString("Filter: ")
	buff.WriteString(f.Expr.ToString())
	buff.WriteString(" ")
	return buff.String()
}
