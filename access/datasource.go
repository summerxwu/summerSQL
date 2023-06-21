package access

import (
	"summerSQL/catalog"
)

type IDataSource interface {
	Schema() *catalog.Schema
	Inhale() (catalog.IBatch, error)
}
