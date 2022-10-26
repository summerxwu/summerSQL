package physical_plan

import (
	"fmt"
)

type MaxAccumulator struct {
	rtValue interface{}
}

func (m *MaxAccumulator) Accumulate(val interface{}) {
	var isBigger = false
	switch _ := m.rtValue.(type) {
	case int64:
		{
			isBigger = val.(int64) > m.rtValue.(int64)
			break
		}
	default:
		{
			panic("data type not supported")
		}
	}
	if isBigger {
		m.rtValue = val
	}
}

func (m *MaxAccumulator) FinalValue() interface{} {
	return m.rtValue
}

type ArrowMaxExpr struct {
	input IPhysicalExpr
}

func NewArrowMaxExpr(input IPhysicalExpr) *ArrowMaxExpr {
	return &ArrowMaxExpr{input: input}
}

func (a *ArrowMaxExpr) InputIs() IPhysicalExpr {
	return a.input
}

func (a *ArrowMaxExpr) CreateAccumulator() IAccumulator {
	return &MaxAccumulator{}
}

func (a *ArrowMaxExpr) ToString() string {
	return fmt.Sprintf("MAX(%s)", a.input.ToString())
}
