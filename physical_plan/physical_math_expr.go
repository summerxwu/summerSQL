package physical_plan

import (
	"summerSQL/catalog"
	"summerSQL/physical_plan/arrow_expr_impl"
)

type AddPhysicalExpr struct {
	BinaryPhysicalExpr
}

func NewAddPhysicalExpr(l IPhysicalExpr, r IPhysicalExpr) *AddPhysicalExpr {
	return &AddPhysicalExpr{
		BinaryPhysicalExpr{
			L:        l,
			R:        r,
			Operator: "+",
			EvalLR:   AddEvalBinaryFunc,
		},
	}
}

func AddEvalBinaryFunc(l catalog.IColumnVector, r catalog.IColumnVector) catalog.IColumnVector {
	switch _ := l.(type) {
	case *catalog.ArrowColumnVector:
		{
			return arrow_expr_impl.ArrowAddEvalFunc(l, r)
		}
	default:
		panic("not a supported column vector type")
	}
	// never reach
	return nil
}
