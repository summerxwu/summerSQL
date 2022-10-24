package physical_plan

import "summerSQL/catalog"

type IAccumulator interface {
	Accumulate(input catalog.IColumnVector)
	FinalValue() catalog.IColumnVector
}
type IAggregatePhysicalExpr interface {
	InputIs() IPhysicalExpr
	CreateAccumulator() IAccumulator
}
