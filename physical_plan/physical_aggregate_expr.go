package physical_plan

type IAccumulator interface {
	Accumulate(val interface{})
	FinalValue() interface{}
}
type IAggregatePhysicalExpr interface {
	InputIs() IPhysicalExpr
	CreateAccumulator() IAccumulator
}
