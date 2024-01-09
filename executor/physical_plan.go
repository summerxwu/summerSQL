package executor

import "summerSQL/catalog"

type IPhysicalPlan interface {
	Schema() catalog.TSchema
	Execute() *catalog.BatchColumns
	ChildNodes() []IPhysicalPlan
	ToString() string
}
