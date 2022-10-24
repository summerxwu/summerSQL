package arrow_expr_impl

import (
	"summerSQL/catalog"
	"summerSQL/physical_plan"
)

type MaxAccumulator struct {
}

func (m *MaxAccumulator) Accumulate(input catalog.IColumnVector) {
}

func (m *MaxAccumulator) FinalValue() catalog.IColumnVector {
	return nil
}

type ArrowMaxExpr struct {
	input physical_plan.IPhysicalExpr
}

func (a *ArrowMaxExpr) InputIs() physical_plan.IPhysicalExpr {
	return a.input
}

func (a *ArrowMaxExpr) CreateAccumulator() physical_plan.IAccumulator {

	return nil
}
