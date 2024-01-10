// Package planner contains the routines about translating the
// logic query plan or expression to the relevant physical one
package planner

import (
	"fmt"
	"strconv"
	"summerSQL/executor"
	logical_plan2 "summerSQL/planner/logical_plan"
)

func CreatePhysicalExpr(expr logical_plan2.ILogicExpr, input logical_plan2.ILogicOperator) (
	executor.IPhysicalExpr, error,
) {
	switch v := expr.(type) {
	case *logical_plan2.ColumnExpr:
		{
			schema := input.Schema()
			index, err := schema.GetIndexByColumnNameCi(v.Name)
			if err != nil {
				return nil, err
			}
			return &executor.ColumnPhysicalExpr{Index: index}, nil
		}
	case *logical_plan2.LiteralStringExpr:
		{
			return executor.NewLiteralStrPhysicalExpr(v.Literal), nil
		}
	case *logical_plan2.LiteralBooleanExpr:
		{
			return executor.NewLiteralBooleanPhysicalExpr(strconv.FormatBool(v.Literal)), nil
		}
	case *logical_plan2.LiteralIntExpr:
		{
			return executor.NewLiteralIntPhysicalExpr(strconv.FormatInt(v.Literal, 10)), nil
		}
	case *logical_plan2.LiteralDoubleExpr:
		{
			return executor.NewLiteralDoublePhysicalExpr(strconv.FormatFloat(v.Literal, 'f', -1, 32)), nil
		}
	case *logical_plan2.Eq:
		{
			l, err := CreatePhysicalExpr(v.L, input)
			if err != nil {
				return nil, err
			}
			r, err := CreatePhysicalExpr(v.R, input)
			if err != nil {
				return nil, err
			}
			return executor.NewEqBinaryPhysicalExpr(l, r), nil
		}
	case *logical_plan2.Neq:
		{
			l, err := CreatePhysicalExpr(v.L, input)
			if err != nil {
				return nil, err
			}
			r, err := CreatePhysicalExpr(v.R, input)
			if err != nil {
				return nil, err
			}
			return executor.NewNeqBinaryPhysicalExpr(l, r), nil
		}
	default:
		panic("not supported logical expression type")
	}
	return nil, nil
}

func CreatePhysicalPlan(lPlan logical_plan2.ILogicOperator) (executor.IPhysicalPlan, error) {
	switch v := lPlan.(type) {
	case *logical_plan2.LogicalScan:
		{
			result := executor.NewScanExec(v.DataSource, &v.Projection)
			return result, nil
		}
	case *logical_plan2.Projection:
		{
			input, err := CreatePhysicalPlan(v.ChildNodes)
			if err != nil {
				return nil, err
			}
			exprs := make([]executor.IPhysicalExpr, 0)
			for _, expr := range v.Expr {
				e, err := CreatePhysicalExpr(expr, v.ChildNodes)
				if err != nil {
					return nil, err
				}
				exprs = append(exprs, e)

			}
			result := &executor.ProjectionExec{
				Input:   input,
				Expr:    exprs,
				PSchema: v.Schema(),
			}
			return result, nil
		}
	case *logical_plan2.LogicalFilter:
		{
			input, err := CreatePhysicalPlan(v.ChildNodes)
			if err != nil {
				return nil, err
			}
			expr, err := CreatePhysicalExpr(v.Expr, v.ChildNodes)
			if err != nil {
				return nil, err
			}
			result := &executor.FilterExec{
				Input: input,
				Expr:  expr,
			}
			return result, nil
		}
	case *logical_plan2.Aggregate:
		{
			input, err := CreatePhysicalPlan(v.ChildNodes)
			if err != nil {
				return nil, err
			}
			groupExpr := make([]executor.IPhysicalExpr, 0)
			for _, expr := range v.GroupExpr {
				e, err := CreatePhysicalExpr(expr, v.ChildNodes)
				if err != nil {
					return nil, err
				}
				groupExpr = append(groupExpr, e)
			}
			aggExpr := make([]executor.IAggregatePhysicalExpr, 0)
			for _, expr := range v.Expr {
				switch p := expr.(type) {
				case *logical_plan2.Max:
					{
						se, err := CreatePhysicalExpr(p.Expr, v.ChildNodes)
						if err != nil {
							return nil, err
						}
						rt := executor.NewArrowMaxExpr(se)
						aggExpr = append(aggExpr, rt)
						break
					}
				default:
					panic(fmt.Sprintf("not supported physical expression, %s", p.ToString()))
				}
			}
			result := &executor.AggregateExec{
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
