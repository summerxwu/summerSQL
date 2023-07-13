package optimizer

import (
	"summerSQL/planner/logical_plan"
)

type IOptimizer interface {
	optimize(plan logical_plan.ILogicPlan) (logical_plan.ILogicPlan, error)
}

var (
	OptimizerRuls []IOptimizer
)

func init() {
	OptimizerRuls = make([]IOptimizer, 0)
	OptimizerRuls = append(OptimizerRuls, NewProjectionPushDownRule())
	OptimizerRuls = append(OptimizerRuls, NewPredictionPushDownRule())
}

func Optimize(plan logical_plan.ILogicPlan) (logical_plan.ILogicPlan, error) {
	optimizedPlan := plan
	for _, rul := range OptimizerRuls {
		optimizedPlan, _ = rul.optimize(optimizedPlan)
	}
	return optimizedPlan, nil

}
