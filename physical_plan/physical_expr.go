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
	_ = builder.Append(l.rawValue)
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
	_ = builder.Append(l.rawValue)
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
	_ = builder.Append(l.rawValue)
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
	_ = builder.Append(l.rawValue)
	retVal.Value = builder.Builder.NewArray()
	return retVal
}

func (l *LiteralDoublePhysicalExpr) ToString() string {
	return fmt.Sprintf("LiteralDouble#PhysicalExpr: %s", l.rawValue)
}
