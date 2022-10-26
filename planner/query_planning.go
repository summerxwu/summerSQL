// Package planner contains the routines about translating the
// logic query plan or expression to the relevant physical one
package planner

import (
	"fmt"
	"strconv"
	"summerSQL/logical_plan"
	"summerSQL/physical_plan"
)

func CreatePhysicalExpr(expr logical_plan.ILogicExpr, input logical_plan.ILogicPlan) (
	physical_plan.IPhysicalExpr, error,
) {
	switch v := expr.(type) {
	case *logical_plan.ColumnExpr:
		{
			schema := input.Schema()
			index, err := schema.GetIndexByColumnNameCi(v.Name)
			if err != nil {
				return nil, err
			}
			return &physical_plan.ColumnPhysicalExpr{Index: index}, nil
		}
	case *logical_plan.LiteralStringExpr:
		{
			return physical_plan.NewLiteralStrPhysicalExpr(v.Literal), nil
		}
	case *logical_plan.LiteralBooleanExpr:
		{
			return physical_plan.NewLiteralBooleanPhysicalExpr(strconv.FormatBool(v.Literal)), nil
		}
	case *logical_plan.LiteralIntExpr:
		{
			return physical_plan.NewLiteralIntPhysicalExpr(strconv.FormatInt(v.Literal, 10)), nil
		}
	case *logical_plan.LiteralDoubleExpr:
		{
			return physical_plan.NewLiteralDoublePhysicalExpr(strconv.FormatFloat(v.Literal, 'f', -1, 32)), nil
		}
	case *logical_plan.Eq:
		{
			l, err := CreatePhysicalExpr(v.L, input)
			if err != nil {
				return nil, err
			}
			r, err := CreatePhysicalExpr(v.R, input)
			if err != nil {
				return nil, err
			}
			return physical_plan.NewEqBinaryPhysicalExpr(l, r), nil
		}
	case *logical_plan.Neq:
		{
			l, err := CreatePhysicalExpr(v.L, input)
			if err != nil {
				return nil, err
			}
			r, err := CreatePhysicalExpr(v.R, input)
			if err != nil {
				return nil, err
			}
			return physical_plan.NewNeqBinaryPhysicalExpr(l, r), nil
		}
	default:
		panic("not supported logical expression type")
	}
	return nil, nil
}

func CreatePhysicalPlan(lPlan logical_plan.ILogicPlan) (physical_plan.IPhysicalPlan, error) {
	switch v := lPlan.(type) {
	case *logical_plan.Scan:
		{
			result := physical_plan.NewScanExec(v.DataSource, &v.Projection)
			return result, nil
		}
	case *logical_plan.Projection:
		{
			input, err := CreatePhysicalPlan(v.ChildNodes)
			if err != nil {
				return nil, err
			}
			exprs := make([]physical_plan.IPhysicalExpr, 0)
			for _, expr := range v.Expr {
				e, err := CreatePhysicalExpr(expr, v.ChildNodes)
				if err != nil {
					return nil, err
				}
				exprs = append(exprs, e)

			}
			result := &physical_plan.ProjectionExec{
				Input: input,
				Expr:  exprs,
			}
			return result, nil
		}
	case *logical_plan.Filter:
		{
			input, err := CreatePhysicalPlan(v.ChildNodes)
			if err != nil {
				return nil, err
			}
			expr, err := CreatePhysicalExpr(v.Expr, v.ChildNodes)
			if err != nil {
				return nil, err
			}
			result := &physical_plan.FilterExec{
				Input: input,
				Expr:  expr,
			}
			return result, nil
		}
	case *logical_plan.Aggregate:
		{
			input, err := CreatePhysicalPlan(v.ChildNodes)
			if err != nil {
				return nil, err
			}
			groupExpr := make([]physical_plan.IPhysicalExpr, 0)
			for _, expr := range v.GroupExpr {
				e, err := CreatePhysicalExpr(expr, v.ChildNodes)
				if err != nil {
					return nil, err
				}
				groupExpr = append(groupExpr, e)
			}
			aggExpr := make([]physical_plan.IAggregatePhysicalExpr, 0)
			for _, expr := range v.Expr {
				switch p := expr.(type) {
				case *logical_plan.Max:
					{
						se, err := CreatePhysicalExpr(p.Expr, v.ChildNodes)
						if err != nil {
							return nil, err
						}
						rt := physical_plan.NewArrowMaxExpr(se)
						aggExpr = append(aggExpr, rt)
						break
					}
				default:
					panic(fmt.Sprintf("not supported physical expression, %s", p.ToString()))
				}
			}
			result := &physical_plan.AggregateExec{
				Input:     input,
				GroupExpr: groupExpr,
				AggExpr:   aggExpr,
			}
			return result, nil

		}
	default:
		panic("not supported logic plan type")
	}
	return nil, nil
}
