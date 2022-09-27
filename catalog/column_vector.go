package catalog

import "github.com/apache/arrow/go/v6/arrow/array"

type IColumnVector interface {
	GetType() IDataTypes
	GetValue(index int) interface{}
	Size() int
}

type ColumnVectorBuilder struct {
	Builder array.Builder
}

type BatchColumns struct {
	BatchSchema Schema
	BatchVector []IColumnVector
}

func (b *BatchColumns) RowCount() int {
	if len(b.BatchVector) != 0 {
		return b.BatchVector[0].Size()
	}
	return 0
}
func (b *BatchColumns) ColumnCount() int {
	return len(b.BatchVector)
}

func (b *BatchColumns) Filed(index int) IColumnVector {
	if index < 0 || index >= len(b.BatchVector) {
		return nil
	}
	return b.BatchVector[index]
}
