package physical_plan

import (
	"fmt"
	"summerSQL/catalog"
)

type IPhysicalExpr interface {
	Evaluate(input catalog.BatchColumns) catalog.IColumnVector
	ToString() string
}

type ColumnPhysicalExpr struct {
	Index int
}

func (c *ColumnPhysicalExpr) Evaluate(input catalog.BatchColumns) catalog.IColumnVector {
	return input.BatchVector[c.Index]
}
func (c *ColumnPhysicalExpr) ToString() string {
	return fmt.Sprintf("Index: %d", c.Index)
}

type LiteralStrPhysicalExpr struct {
	rawValue string
}

func (l *LiteralStrPhysicalExpr) Evaluate(input catalog.BatchColumns) catalog.IColumnVector {
	column := catalog.NewColumn(catalog.NewArrowStringType(), "None", input.RowCount())
	retVal := catalog.NewArrowColumnVector(column)
	builder := catalog.NewArrowColumnVectorBuilder(column)
	for i := 0; i < input.RowCount(); i++ {
		_ = builder.Append(l.rawValue)
	}
	retVal.Value = builder.Builder.NewArray()
	return retVal
}

func (l *LiteralStrPhysicalExpr) ToString() string {
	return fmt.Sprintf("LiteralStr#PhysicalExpr: %s", l.rawValue)
}

type LiteralIntPhysicalExpr struct {
	rawValue string
}

func (l *LiteralIntPhysicalExpr) Evaluate(input catalog.BatchColumns) catalog.IColumnVector {
	column := catalog.NewColumn(catalog.NewArrowIntType(), "None", input.RowCount())
	retVal := catalog.NewArrowColumnVector(column)
	builder := catalog.NewArrowColumnVectorBuilder(column)
	for i := 0; i < input.RowCount(); i++ {
		_ = builder.Append(l.rawValue)
	}
	retVal.Value = builder.Builder.NewArray()
	return retVal
}

func (l *LiteralIntPhysicalExpr) ToString() string {
	return fmt.Sprintf("LiteralInt#PhysicalExpr: %s", l.rawValue)
}

type LiteralBooleanPhysicalExpr struct {
	rawValue string
}

func (l *LiteralBooleanPhysicalExpr) Evaluate(input catalog.BatchColumns) catalog.IColumnVector {
	column := catalog.NewColumn(catalog.NewArrowBoolType(), "None", input.RowCount())
	retVal := catalog.NewArrowColumnVector(column)
	builder := catalog.NewArrowColumnVectorBuilder(column)
	for i := 0; i < input.RowCount(); i++ {
		_ = builder.Append(l.rawValue)
	}
	retVal.Value = builder.Builder.NewArray()
	return retVal
}

func (l *LiteralBooleanPhysicalExpr) ToString() string {
	return fmt.Sprintf("LiteralBool#PhysicalExpr: %s", l.rawValue)
}

type LiteralDoublePhysicalExpr struct {
	rawValue string
}

func (l *LiteralDoublePhysicalExpr) Evaluate(input catalog.BatchColumns) catalog.IColumnVector {
	column := catalog.NewColumn(catalog.NewArrowDoubleType(), "None", input.RowCount())
	retVal := catalog.NewArrowColumnVector(column)
	builder := catalog.NewArrowColumnVectorBuilder(column)
	for i := 0; i < input.RowCount(); i++ {
		_ = builder.Append(l.rawValue)
	}
	retVal.Value = builder.Builder.NewArray()
	return retVal
}

func (l *LiteralDoublePhysicalExpr) ToString() string {
	return fmt.Sprintf("LiteralDouble#PhysicalExpr: %s", l.rawValue)
}

type EvalBinaryFunc = func(l catalog.IColumnVector, r catalog.IColumnVector) catalog.IColumnVector

type BinaryPhysicalExpr struct {
	L        IPhysicalExpr
	R        IPhysicalExpr
	Operator string
	EvalLR   EvalBinaryFunc
}

func (b *BinaryPhysicalExpr) Evaluate(input catalog.BatchColumns) catalog.IColumnVector {
	lr := b.L.Evaluate(input)
	rr := b.R.Evaluate(input)
	if lr.Size() != rr.Size() {
		panic("operands size different")
	}
	if lr.GetType().Name() != rr.GetType().Name() {
		panic("operands return type different not support")
	}
	if b.EvalLR == nil {
		panic("unspecified EvalFunction")
	}
	return b.EvalLR(lr, rr)
}
func (b *BinaryPhysicalExpr) ToString() string {
	return fmt.Sprintf("Binary#PhysicalExpr: %s %s %s", b.L.ToString(), b.Operator, b.R.ToString())
}
