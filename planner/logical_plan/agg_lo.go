package logical_plan

import (
	"bytes"
	"summerSQL/catalog"
)

type Aggregate struct {
	ChildNodes ILogicOperator
	Expr       []ILogicExpr
	GroupExpr  []ILogicExpr
}

func NewAggregate(childNodes ILogicOperator, expr []ILogicExpr, groupExpr []ILogicExpr) *Aggregate {
	return &Aggregate{ChildNodes: childNodes, Expr: expr, GroupExpr: groupExpr}
}

func (a *Aggregate) Schema() *catalog.TSchema {
	//todo: fetch the schema of aggregate plan directly
	return nil
}

func (a *Aggregate) Children() []ILogicOperator {
	cd := make([]ILogicOperator, 0)
	cd = append(cd, a.ChildNodes)
	return cd
}

func (a *Aggregate) ToString() string {
	buff := bytes.Buffer{}
	buff.WriteString("Aggregate:\n")
	for _, expr := range a.Expr {
		buff.WriteString("  agrgt=> ")
		buff.WriteString(expr.ToString())
		buff.WriteString("\n")
	}
	for _, expr := range a.GroupExpr {
		buff.WriteString("  group=> ")
		buff.WriteString(expr.ToString())
		buff.WriteString("\n")
	}
	return buff.String()
}
