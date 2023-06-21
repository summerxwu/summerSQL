package executor

import "summerSQL/catalog"

type IPhysicalPlan interface {
	Schema() catalog.Schema
	Execute() *catalog.BatchColumns
	ChildNodes() []IPhysicalPlan
	ToString() string
}
