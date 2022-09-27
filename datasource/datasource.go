package datasource

import "awesomeProject/catalog"

type IDataSource interface {
	Schema() catalog.Schema
	Scan(projections catalog.Schema) catalog.BatchColumns
}
