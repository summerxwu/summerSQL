package catalog

import "errors"

type Schema struct {
	Fields []*Column
}

var (
	ColumnDefineMissing = errors.New("can't find column define")
)

func (s *Schema) GetIndexByColumn(in *Column) (int, error) {
	for index, column := range s.Fields {
		if in.Name == column.Name {
			return index, nil
		}
	}
	return -1, ColumnDefineMissing
}
