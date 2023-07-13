package planner

import (
	"github.com/pingcap/tidb/parser/ast"
	"summerSQL/planner/logical_plan"
)

// createSelectLogicalPlan deal the normal SPJ query for now
func createSelectLogicalPlan(selectStmt *ast.SelectStmt) logical_plan.ILogicPlan {
	var plan logical_plan.ILogicPlan
	// from clause
	fromNode := selectStmt.From
	if fromNode != nil && fromNode.TableRefs.Tp != 0 {
		// TODO: do not support JOIN for now

		// create table scan node
		relName := fromNode.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.L
		plan, _ = logical_plan.NewScan(relName)
	}
	// where clause
	whereNode := selectStmt.Where
	if whereNode != nil {

		logical_plan.NewFilter(plan)

	}
	// project clause

	return plan
}

func CreateLogicalPlan(stmt []ast.StmtNode) logical_plan.ILogicPlan {
	switch v := stmt[0].(type) {
	case *ast.SelectStmt:
		{
			return createSelectLogicalPlan(v)
		}
	default:
		break
	}
	return nil
}
