package logical_plan

import (
	"bytes"
	"summerSQL/catalog"
)

type Projector struct {
	Projections []ILogicExpr     `json:"expressions,omitempty"`
	ChildPlan   []ILogicOperator `json:"childrens,omitempty"`
}

func NewProjector(childPlan []ILogicOperator, projections []ILogicExpr) *Projector {
	return &Projector{projections, childPlan}
}

func (p *Projector) ToString() string {
	buff := bytes.Buffer{}
	buff.WriteString("Projection: ")
	for _, expr := range p.Projections {
		buff.WriteString(expr.ToString())
		buff.WriteString(", ")
	}
	return buff.String()
}
func (p *Projector) Schema() *catalog.TSchema {
	return nil
}

func (p *Projector) Children() []ILogicOperator {
	return p.ChildPlan
}
