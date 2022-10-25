package physical_plan

import (
	"summerSQL/catalog"
)

type EqBinaryPhysicalExpr struct {
	BinaryPhysicalExpr
}

func NewEqBinaryPhysicalExpr(l IPhysicalExpr, r IPhysicalExpr) *EqBinaryPhysicalExpr {
	return &EqBinaryPhysicalExpr{
		BinaryPhysicalExpr{
			L:        l,
			R:        r,
			Operator: "=",
			EvalLR:   EqEvalBinaryFunc,
		},
	}
}
func EqEvalBinaryFunc(l catalog.IColumnVector, r catalog.IColumnVector) catalog.IColumnVector {
	switch _ := l.(type) {
	case *catalog.ArrowColumnVector:
		{
			return ArrowEqBinaryFunc(l.(*catalog.ArrowColumnVector), r.(*catalog.ArrowColumnVector))
		}
	default:
		{
			panic("not a supported column vector type")
		}
	}

	return nil
}

type NeqBinaryPhysicalExpr struct {
	BinaryPhysicalExpr
}

func NewNeqBinaryPhysicalExpr(l IPhysicalExpr, r IPhysicalExpr) *EqBinaryPhysicalExpr {
	return &EqBinaryPhysicalExpr{
		BinaryPhysicalExpr{
			L:        l,
			R:        r,
			Operator: "=",
			EvalLR:   NeqEvalBinaryFunc,
		},
	}
}

func NeqEvalBinaryFunc(l catalog.IColumnVector, r catalog.IColumnVector) catalog.IColumnVector {
	switch _ := l.(type) {
	case *catalog.ArrowColumnVector:
		{
			return ArrowNeqBinaryFunc(l.(*catalog.ArrowColumnVector), r.(*catalog.ArrowColumnVector))
		}
	default:
		{
			panic("not a supportted column vector type")
		}
	}

	return nil
}

// TODO: More compare expr implemented
