package portal

import (
	"summerSQL/executor"
	"summerSQL/server"
	"vitess.io/vitess/go/sqltypes"
)

func ExecutePlan(plan executor.IPhysicalPlan) sqltypes.Result {
	return *server.SelectRowsResult
}
