package logicplan

import "summerSQL/catalog"

type ILogicExpr interface {
	// ReturnType get the value type which evaluated by current expression
	// input is not required for some expression type
	ReturnType(input ILogicPlan) catalog.IDataTypes
	ToString() string
}
