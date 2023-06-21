package executor

import (
	"github.com/apache/arrow/go/v11/arrow/array"
	"summerSQL/catalog"
)

func arrowInt64AddFunc(l catalog.Batch, r catalog.Batch) *catalog.Batch {
	column := catalog.NewColumn(catalog.NewArrowIntType(), "None", 0)
	result := catalog.NewArrowColumnVector(column)
	builder := catalog.NewArrowColumnVectorBuilder(column)
	realBdr := builder.Builder.(*array.Int64Builder)

	for i := 0; i < l.Size(); i++ {
		lv := l.GetValue(i).(int64)
		rv := r.GetValue(i).(int64)
		realBdr.Append(lv + rv)
	}
	result.Value = realBdr.NewArray()
	return result
}
func arrowStrAddFunc(l catalog.Batch, r catalog.Batch) *catalog.Batch {
	column := catalog.NewColumn(catalog.NewArrowStringType(), "None", 0)
	result := catalog.NewArrowColumnVector(column)
	builder := catalog.NewArrowColumnVectorBuilder(column)
	realBdr := builder.Builder.(*array.StringBuilder)

	for i := 0; i < l.Size(); i++ {
		lv := l.GetValue(i).(string)
		rv := r.GetValue(i).(string)
		realBdr.Append(lv + rv)
	}
	result.Value = realBdr.NewArray()
	return result
}

func ArrowAddEvalFunc(l catalog.IStrip, r catalog.IStrip) catalog.IStrip {
	switch l.GetType().(type) {
	case *catalog.IntType:
		{
			return arrowInt64AddFunc(*(l.(*catalog.Batch)), *(r.(*catalog.Batch)))
		}
	case *catalog.StringType:
		{

			return *arrowStrAddFunc(*(l.(*catalog.Batch)), *(r.(*catalog.Batch)))
		}
	default:
		panic("datatype not supported")
	}
	// never reach
	return nil
}
