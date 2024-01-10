package logical_plan

import (
	"bytes"
	"fmt"
)

type LogicalFilter struct {
	SubOperator ILogicOperator
}

func NewFilter(operator ILogicOperator) *LogicalFilter {
	return &LogicalFilter{SubOperator: operator}
}

func (l *LogicalFilter) Build() error {
	return l.SubOperator.Build()
}

func (l *LogicalFilter) VisitSubOperator(closure Closure) error {
	return VisitOperatorTrees(closure, l.SubOperator)
}

func (l *LogicalFilter) ToString() string {
	buffer := bytes.Buffer{}
	buffer.WriteString(fmt.Sprintf("\tFilter: "))
	err := VisitOperatorTrees(func(operator ILogicOperator) (bool, error) {
		buffer.WriteString(operator.ToString())
		return true, nil
	}, l.SubOperator)
	if err != nil {
		panic(err)
	}
	return buffer.String()
}
