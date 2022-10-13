package catalog

import (
	"fmt"
	"github.com/apache/arrow/go/v6/arrow"
	"github.com/apache/arrow/go/v6/arrow/array"
	"github.com/apache/arrow/go/v6/arrow/memory"
)

type ArrowColumnVector struct {
	ColumnSpec Column
	Length     int
	Value      array.Interface
}

func (a ArrowColumnVector) GetType() IDataTypes {
	return a.ColumnSpec.Type
}

func (a ArrowColumnVector) GetValue(index int) interface{} {
	upperBound := a.Value.Len()
	if index < 0 || index >= upperBound {
		return fmt.Errorf("out of bound")
	}
	if a.Value.IsNull(index) {
		return nil
	}
	switch v := a.Value.(type) {
	case *array.String:
		{
			return v.Value(index)
		}
	case *array.Int64:
		{
			return v.Value(index)
		}
	case *array.Boolean:
		{
			return v.Value(index)
		}
	case *array.Decimal128:
		{
			return v.Value(index)
		}
	}
	return fmt.Errorf("invalid type index fetch")
}

func (a ArrowColumnVector) Size() int {
	return a.Length
}

func (a ArrowColumnVector) Print() (string, error) {
	switch v := a.Value.(type) {
	case *array.String:
		{
			return v.String(), nil
		}
	case *array.Int64:
		{
			return v.String(), nil
		}
	case *array.Boolean:
		{
			return v.String(), nil
		}
	case *array.Decimal128:
		{
			return v.String(), nil
		}
	}
	return "", UnsupportedType
}

func NewArrowColumnVector(column *Column) *ArrowColumnVector {
	return &ArrowColumnVector{ColumnSpec: *column, Length: 0, Value: nil}
}

func NewArrowColumnVectorBuilder(column *Column) *ColumnVectorBuilder {
	switch column.Type.(type) {
	case *IntType:
		{
			return &ColumnVectorBuilder{
				Builder: array.NewBuilder(memory.NewGoAllocator(), &arrow.Int64Type{}),
			}
		}
	case *StringType:
		{
			return &ColumnVectorBuilder{
				Builder: array.NewBuilder(memory.NewGoAllocator(), &arrow.StringType{}),
			}
		}
	case *DoubleType:
		{
			return &ColumnVectorBuilder{
				Builder: array.NewBuilder(memory.NewGoAllocator(), &arrow.Decimal128Type{}),
			}
		}
	case *BoolType:
		{
			return &ColumnVectorBuilder{
				Builder: array.NewBuilder(memory.NewGoAllocator(), &arrow.BooleanType{}),
			}
		}
	}
	return nil
}
