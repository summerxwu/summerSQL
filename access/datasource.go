package access

import (
	"summerSQL/catalog"
)

type IDataSource interface {
	Schema() *catalog.TSchema
	Inhale() (catalog.IBatch, error)
}
