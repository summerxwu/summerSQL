package logicplan

import (
	"fmt"
	"summerSQL/catalog"
)

// ColumnExpr define the reference about a relation in query plan
type ColumnExpr struct {
	Name string
}

func (c *ColumnExpr) ReturnType(input ILogicPlan) catalog.IDataTypes {
	for _, field := range input.Schema().Fields {
		if field.Name == c.Name {
			return field.Type
		}
	}
	return nil
}
func (c *ColumnExpr) ToString() string {
	return fmt.Sprintf("Column#Expr: %s", c.Name)
}

type LiteralStringExpr struct {
	Literal string
}

func (l *LiteralStringExpr) ReturnType(input ILogicPlan) catalog.IDataTypes {
	return catalog.NewArrowStringType()

}
func (l *LiteralStringExpr) ToString() string {
	return fmt.Sprintf("LitrlStr#Expr: %s", l.Literal)
}

type LiteralIntExpr struct {
	Literal int64
}

func (l *LiteralIntExpr) ReturnType(input ILogicPlan) catalog.IDataTypes {
	return catalog.NewArrowIntType()

}
func (l *LiteralIntExpr) ToString() string {
	return fmt.Sprintf("LitrlInt#Expr: %v", l.Literal)
}

type LiteralDoubleExpr struct {
	Literal float64
}

func (l *LiteralDoubleExpr) ReturnType(input ILogicPlan) catalog.IDataTypes {
	return catalog.NewArrowDoubleType()
}

func (l *LiteralDoubleExpr) ToString() string {
	return fmt.Sprintf("LiterlDoubel#Expr: %v", l.Literal)
}

type LiteralBooleanExpr struct {
	Literal bool
}

func (l *LiteralBooleanExpr) ReturnType(input ILogicPlan) catalog.IDataTypes {
	return catalog.NewArrowBoolType()
}

func (l *LiteralBooleanExpr) ToString() string {
	return fmt.Sprintf("LiterlBoolean#Expr: %v", l.Literal)
}

type BinaryExpr struct {
	ILogicExpr
	Name     string
	Operator string
	L        ILogicExpr
	R        ILogicExpr
}

type BooleanBinaryExpr struct {
	BinaryExpr
}

func (b *BooleanBinaryExpr) ReturnType(input ILogicPlan) catalog.IDataTypes {
	return catalog.NewArrowBoolType()
}

func (b *BooleanBinaryExpr) ToString() string {
	return fmt.Sprintf("BooleanBinary#Expr: %s %s %s", b.L, b.Operator, b.R)
}

type MathBinaryExpr struct {
	BinaryExpr
}

func (m *MathBinaryExpr) ReturnType(input ILogicPlan) catalog.IDataTypes {
	return m.L.ReturnType(input)
}

func (m *MathBinaryExpr) ToString() string {
	return fmt.Sprintf("MathBinary#Expr: %s %s %s", m.L, m.Operator, m.R)
}

type Eq struct {
	BooleanBinaryExpr
}
type Neq struct {
	BooleanBinaryExpr
}
type Gt struct {
	BooleanBinaryExpr
}
type GtEq struct {
	BooleanBinaryExpr
}
type Lt struct {
	BooleanBinaryExpr
}
type LtEq struct {
	BooleanBinaryExpr
}
type And struct {
	BooleanBinaryExpr
}
type Or struct {
	BooleanBinaryExpr
}

type Add struct {
	MathBinaryExpr
}
type Sub struct {
	MathBinaryExpr
}
type Multi struct {
	MathBinaryExpr
}
type Divide struct {
	MathBinaryExpr
}
type Mod struct {
	MathBinaryExpr
}

type AggregateExpr struct {
	Name string
	Expr ILogicExpr
}

func (a *AggregateExpr) ReturnType(input ILogicPlan) catalog.IDataTypes {
	return a.Expr.ReturnType(input)
}

func (a *AggregateExpr) ToString() string {
	return fmt.Sprintf("Aggregate#Expr: %s(%s)", a.Name, a.Expr.ToString())
}

type Min struct {
	AggregateExpr
}
type Max struct {
	AggregateExpr
}
type Sum struct {
	AggregateExpr
}
type Avg struct {
	AggregateExpr
}

type Count struct {
	AggregateExpr
}

func (c *Count) ReturnType(input ILogicPlan) catalog.IDataTypes {
	return catalog.NewArrowIntType()
}
