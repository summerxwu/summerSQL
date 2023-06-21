package logical_plan

import (
	"fmt"
	"github.com/apache/arrow/go/v11/arrow"
	"summerSQL/catalog"
)

type ILogicExpr interface {
	// ReturnType get the value type which evaluated by current expression
	// input is not required for some expression type
	ReturnType(input ILogicPlan) catalog.IDataTypes
	ToString() string
}

// ColumnExpr define the reference about a relation in query plan
type ColumnExpr struct {
	Name string
}

func (c *ColumnExpr) ReturnType(input ILogicPlan) catalog.IDataTypes {
	for _, field := range input.Schema().Fields() {
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

func (l *LiteralStringExpr) ReturnType(ILogicPlan) catalog.IDataTypes {
	return &arrow.StringType{}

}
func (l *LiteralStringExpr) ToString() string {
	return fmt.Sprintf("LitrlStr#Expr: \"%s\"", l.Literal)
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

func NewBooleanBinaryExpr(name string, operator string, l ILogicExpr, r ILogicExpr) *BooleanBinaryExpr {
	return &BooleanBinaryExpr{
		BinaryExpr: BinaryExpr{
			Name:     name,
			Operator: operator,
			L:        l,
			R:        r,
		},
	}
}

func (b *BooleanBinaryExpr) ReturnType(ILogicPlan) catalog.IDataTypes {
	return &arrow.BooleanType{}
}

func (b *BooleanBinaryExpr) ToString() string {
	return fmt.Sprintf("BooleanBinary#Expr: %s [%s] %s", b.L.ToString(), b.Operator, b.R.ToString())
}

type MathBinaryExpr struct {
	BinaryExpr
}

func NewMathBinaryExpr(name string, operator string, l ILogicExpr, r ILogicExpr) *MathBinaryExpr {
	return &MathBinaryExpr{
		BinaryExpr: BinaryExpr{
			Name:     name,
			Operator: operator,
			L:        l,
			R:        r,
		},
	}
}

func (m *MathBinaryExpr) ReturnType(input ILogicPlan) catalog.IDataTypes {
	return m.L.ReturnType(input)
}

func (m *MathBinaryExpr) ToString() string {
	return fmt.Sprintf("MathBinary#Expr: %s %s %s", m.L.ToString(), m.Operator, m.R.ToString())
}

type Eq struct {
	BooleanBinaryExpr
}

func NewEq(l ILogicExpr, r ILogicExpr) *Eq {
	be := NewBooleanBinaryExpr("Eq", "=", l, r)
	return &Eq{*be}
}

type Neq struct {
	BooleanBinaryExpr
}

func NewNeq(l ILogicExpr, r ILogicExpr) *Neq {
	be := NewBooleanBinaryExpr("Neq", "!=", l, r)
	return &Neq{*be}
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

func (c *Count) ReturnType(ILogicPlan) catalog.IDataTypes {
	return &arrow.Uint64Type{}
}
