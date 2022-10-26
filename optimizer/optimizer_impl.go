package optimizer

import (
	"summerSQL/catalog"
	"summerSQL/logical_plan"
)

type ProjectionPushDownRule struct {
	ExtractedColumn map[string]*catalog.Column
}

func NewProjectionPushDownRule() *ProjectionPushDownRule {
	return &ProjectionPushDownRule{ExtractedColumn: make(map[string]*catalog.Column)}
}

func (p *ProjectionPushDownRule) extractColumn(plan logical_plan.ILogicPlan) error {
	switch v := plan.(type) {
	case *logical_plan.Projection:
		{
			err := p.extract(v.Expr)
			if err != nil {
				panic(err.Error())
			}
			return nil
		}
	default:
		for _, logicPlan := range v.Children() {
			err := p.extractColumn(logicPlan)
			if err != nil {
				panic(err.Error())
			}
		}
	}
	return nil
}

func (p *ProjectionPushDownRule) extract(exprs []logical_plan.ILogicExpr) error {
	for _, expr := range exprs {
		switch v := expr.(type) {
		case *logical_plan.ColumnExpr:
			{
				p.ExtractedColumn[v.Name] = nil
				break
			}
		case *logical_plan.AggregateExpr:
			{
				err := p.extract([]logical_plan.ILogicExpr{v.Expr})
				if err != nil {
					return err
				}
				break
			}
		case *logical_plan.BinaryExpr:
			{
				// TODO: confirm bool binary expr and math binary expr can fall here
				err := p.extract([]logical_plan.ILogicExpr{v.L})
				if err != nil {
					return err
				}
				err = p.extract([]logical_plan.ILogicExpr{v.R})
				if err != nil {
					return err
				}
				break
			}
		default:
			continue
		}
	}

	return nil
}

func (p *ProjectionPushDownRule) pushDown(plan logical_plan.ILogicPlan) error {
	switch v := plan.(type) {
	case *logical_plan.Scan:
		{
			projections := &catalog.Schema{Fields: make([]*catalog.Column, 0)}
			for s := range p.ExtractedColumn {
				index, err := v.Schema().GetIndexByColumnNameCi(s)
				if err != nil {
					return err
				}
				projections.Fields = append(projections.Fields, v.Schema().Fields[index])
			}
			v.Projection = *projections
			break
		}
	default:
		for _, logicPlan := range plan.Children() {
			err := p.pushDown(logicPlan)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *ProjectionPushDownRule) Optimize(plan logical_plan.ILogicPlan) (logical_plan.ILogicPlan, error) {
	err := p.extractColumn(plan)
	if err != nil {
		return nil, err
	}
	err = p.pushDown(plan)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

type PredictionPushDownRule struct {
	ExtractedExpr logical_plan.ILogicExpr
}

func NewPredictionPushDownRule() *PredictionPushDownRule {
	return &PredictionPushDownRule{ExtractedExpr: nil}
}

func (p *PredictionPushDownRule) Optimize(plan logical_plan.ILogicPlan) (logical_plan.ILogicPlan, error) {
	return nil, nil
}