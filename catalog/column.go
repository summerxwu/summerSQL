package catalog

type Column struct {
	Type  IDataTypes
	Name  string
	Index int
}

func NewColumn(typeIn IDataTypes, name string, index int) *Column {
	return &Column{Type: typeIn, Name: name, Index: index}
}
