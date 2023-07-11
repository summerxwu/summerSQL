package planner

import (
	"github.com/pingcap/tidb/parser/ast"
	"summerSQL/planner/logical_plan"
)

func createSelectLogicalPlan(selectStmt *ast.SelectStmt) logical_plan.ILogicPlan {
	return nil
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
