package catalog

import (
	"fmt"
	"github.com/apache/arrow/go/v6/arrow/array"
	"github.com/apache/arrow/go/v6/arrow/decimal128"
	"strconv"
)

type IColumnVector interface {
	GetType() IDataTypes
	GetValue(index int) interface{}
	Size() int
	Print() (string, error)
}

// ColumnVectorBuilder
// TODO: Detach the Arrow.array builder from the Abstract ColumnVectorBuilder
type ColumnVectorBuilder struct {
	Builder array.Builder
}

func (c *ColumnVectorBuilder) Append(val string) error {
	if len(val) == 0 {
		c.Builder.AppendNull()
		return nil
	}
	switch v := c.Builder.(type) {
	case *array.StringBuilder:
		{
			v.Append(val)
			break
		}
	case *array.Int64Builder:
		{
			rtVal, err := strconv.ParseInt(val, 10, 64)
			if err == nil {
				v.Append(rtVal)
				break
			}
			return err
		}
	case *array.Decimal128Builder:
		{
			rtVal, err := strconv.ParseInt(val, 10, 64)
			if err == nil {
				v.Append(decimal128.FromI64(rtVal))
				break
			}
			return err

		}
	case *array.BooleanBuilder:
		{
			rtVal, err := strconv.ParseBool(val)
			if err == nil {
				v.Append(rtVal)
				break
			}
			return err

		}
	}
	return nil
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

func (b *BatchColumns) Print() {
	for _, vector := range b.BatchVector {
		str, err := vector.Print()
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		fmt.Println(str)
	}
}
