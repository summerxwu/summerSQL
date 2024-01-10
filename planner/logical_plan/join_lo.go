package logical_plan

import (
	"bytes"
	"fmt"
	"summerSQL/catalog"
)

type JoinType int8

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	CrossJoin
)

type Join struct {
	ChildPlan  []ILogicOperator
	Conditions []ILogicExpr
	Type       JoinType
}

func NewJoinPlan(childPlan []ILogicOperator, cond []ILogicExpr, tp JoinType) *Join {
	return &Join{childPlan, cond, tp}
}

func (j *Join) Schema() *catalog.TSchema {
	return nil
}

func (j *Join) Children() []ILogicOperator {
	return j.ChildPlan
}

func (j *Join) ToString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("Join: %v\n", j.Type))
	buffer.WriteString(fmt.Sprintf("\t cond: %v\n", j.Conditions[0].ToString()))
	buffer.WriteString(fmt.Sprintf("\t LeftTable: %v\n", j.ChildPlan[0].ToString()))
	buffer.WriteString(fmt.Sprintf("\t RightTable: %v\n", j.ChildPlan[1].ToString()))
	return buffer.String()
}
