package datasource

import "summerSQL/catalog"

type IDataSource interface {
	Schema() catalog.Schema
	Scan(projections catalog.Schema) (catalog.BatchColumns, error)
	Info() string
}
