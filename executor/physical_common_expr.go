package executor

import (
	"fmt"
	"summerSQL/catalog"
)

type IPhysicalExpr interface {
	Evaluate(input catalog.IBatch) catalog.IStrip
	ToString() string
}

type ColumnPhysicalExpr struct {
	Index int
}

func NewColumnPhysicalExpr(index int) *ColumnPhysicalExpr {
	return &ColumnPhysicalExpr{Index: index}
}

func (c *ColumnPhysicalExpr) Evaluate(input catalog.IBatch) catalog.IStrip {
	return input.BatchVector[c.Index]
}
func (c *ColumnPhysicalExpr) ToString() string {
	return fmt.Sprintf("Index: %d", c.Index)
}

type LiteralStrPhysicalExpr struct {
	rawValue string
}

func NewLiteralStrPhysicalExpr(rawValue string) *LiteralStrPhysicalExpr {
	return &LiteralStrPhysicalExpr{rawValue: rawValue}
}

func (l *LiteralStrPhysicalExpr) Evaluate(input catalog.BatchColumns) catalog.IStrip {
	column := catalog.NewColumn(catalog.NewArrowStringType(), "None", 0)
	retVal := catalog.NewArrowColumnVector(column)
	retVal.Length = input.RowCount()
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

func NewLiteralIntPhysicalExpr(rawValue string) *LiteralIntPhysicalExpr {
	return &LiteralIntPhysicalExpr{rawValue: rawValue}
}

func (l *LiteralIntPhysicalExpr) Evaluate(input catalog.BatchColumns) catalog.IStrip {
	column := catalog.NewColumn(catalog.NewArrowIntType(), "None", 0)
	retVal := catalog.NewArrowColumnVector(column)
	retVal.Length = input.RowCount()
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

func NewLiteralBooleanPhysicalExpr(rawValue string) *LiteralBooleanPhysicalExpr {
	return &LiteralBooleanPhysicalExpr{rawValue: rawValue}
}

func (l *LiteralBooleanPhysicalExpr) Evaluate(input catalog.BatchColumns) catalog.IStrip {
	column := catalog.NewColumn(catalog.NewArrowBoolType(), "None", 0)
	retVal := catalog.NewArrowColumnVector(column)
	retVal.Length = input.RowCount()
	builder := catalog.NewArrowColumnVectorBuilder(column)
	for i := 0; i < input.RowCount(); i++ {
		_ = builder.StrAppend(l.rawValue)
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

func NewLiteralDoublePhysicalExpr(rawValue string) *LiteralDoublePhysicalExpr {
	return &LiteralDoublePhysicalExpr{rawValue: rawValue}
}

func (l *LiteralDoublePhysicalExpr) Evaluate(input catalog.BatchColumns) catalog.IStrip {
	column := catalog.NewColumn(catalog.NewArrowDoubleType(), "None", 0)
	retVal := catalog.NewArrowColumnVector(column)
	retVal.Length = input.RowCount()
	builder := catalog.NewArrowColumnVectorBuilder(column)
	for i := 0; i < input.RowCount(); i++ {
		_ = builder.StrAppend(l.rawValue)
	}
	retVal.Value = builder.Builder.NewArray()
	return retVal
}

func (l *LiteralDoublePhysicalExpr) ToString() string {
	return fmt.Sprintf("LiteralDouble#PhysicalExpr: %s", l.rawValue)
}

type EvalBinaryFunc = func(l catalog.IStrip, r catalog.IStrip) catalog.IStrip

type BinaryPhysicalExpr struct {
	L        IPhysicalExpr
	R        IPhysicalExpr
	Operator string
	EvalLR   EvalBinaryFunc
}

func (b *BinaryPhysicalExpr) Evaluate(input catalog.BatchColumns) catalog.IStrip {
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
