package executor

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
func EqEvalBinaryFunc(l catalog.IStrip, r catalog.IStrip) catalog.IStrip {
	switch l.(type) {
	case *catalog.Batch:
		{
			return ArrowEqBinaryFunc(l.(*catalog.Batch), r.(*catalog.Batch))
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

func NeqEvalBinaryFunc(l catalog.IStrip, r catalog.IStrip) catalog.IStrip {
	switch l.(type) {
	case *catalog.Batch:
		{
			return ArrowNeqBinaryFunc(l.(*catalog.Batch), r.(*catalog.Batch))
		}
	default:
		{
			panic("not a supportted column vector type")
		}
	}

	return nil
}

// TODO: More compare expr implemented
