package physical_plan

import (
	"github.com/apache/arrow/go/v6/arrow/array"
	"summerSQL/catalog"
)

func arrowInt64AddFunc(l catalog.ArrowColumnVector, r catalog.ArrowColumnVector) *catalog.ArrowColumnVector {
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
func arrowStrAddFunc(l catalog.ArrowColumnVector, r catalog.ArrowColumnVector) *catalog.ArrowColumnVector {
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

func ArrowAddEvalFunc(l catalog.IColumnVector, r catalog.IColumnVector) catalog.IColumnVector {
	switch l.GetType().(type) {
	case *catalog.IntType:
		{
			return arrowInt64AddFunc(l.(catalog.ArrowColumnVector), r.(catalog.ArrowColumnVector))
		}
	case *catalog.StringType:
		{

			return arrowStrAddFunc(l.(catalog.ArrowColumnVector), r.(catalog.ArrowColumnVector))
		}
	default:
		panic("datatype not supported")
	}
	// never reach
	return nil
}
