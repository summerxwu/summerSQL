package catalog

import (
	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
)

// IStrip is a datatype which represent a single column bounded with data
type IStrip = arrow.Array

type TBooleanStrip = array.Boolean

// IStripBuilder provides the API to create strip like value array
type IStripBuilder = array.Builder

func NewStripBuilder(mem memory.Allocator, dtype IDataTypes) IStripBuilder {
	return array.NewBuilder(mem, dtype)
}
