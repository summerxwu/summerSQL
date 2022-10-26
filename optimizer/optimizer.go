package optimizer

import "summerSQL/logical_plan"

var (
	OptimizerRuls []IOptimizer
)

func init() {
	OptimizerRuls = make([]IOptimizer, 0)
	OptimizerRuls = append(OptimizerRuls, NewProjectionPushDownRule())
	OptimizerRuls = append(OptimizerRuls, NewPredictionPushDownRule())
}

type IOptimizer interface {
	Optimize(plan logical_plan.ILogicPlan) (logical_plan.ILogicPlan, error)
}
