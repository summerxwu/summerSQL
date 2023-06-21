package catalog

import (
	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
)

// IBatch is datatype represents one or more strips bounded together
type IBatch = arrow.Record

func NewBatch(schema *Schema, cols []IStrip, nrows int64) IBatch {
	return array.NewRecord(schema, cols, nrows)
}
