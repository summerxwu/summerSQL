package executor

import (
	"summerSQL/catalog"
)

func ArrowEqBinaryFunc(l *catalog.Batch, r *catalog.Batch) *catalog.Batch {
	var err error
	column := catalog.NewColumn(catalog.NewArrowBoolType(), "None", 0)
	result := catalog.NewArrowColumnVector(column)
	builder := catalog.NewArrowColumnVectorBuilder(column)
	for i := 0; i < l.Size(); i++ {
		lv := l.GetValue(i)
		rv := r.GetValue(i)
		if rv == lv {
			err = builder.StrAppend("1")
		} else {
			err = builder.StrAppend("0")
		}
		result.Length++
		if err != nil {
			panic("Expr Eval failed")
		}
	}
	result.Value = builder.Builder.NewArray()
	return result
}

func ArrowNeqBinaryFunc(l *catalog.Batch, r *catalog.Batch) *catalog.Batch {
	var err error
	column := catalog.NewColumn(catalog.NewArrowBoolType(), "None", 0)
	result := catalog.NewArrowColumnVector(column)
	builder := catalog.NewArrowColumnVectorBuilder(column)
	for i := 0; i < l.Size(); i++ {
		lv := l.GetValue(i)
		rv := r.GetValue(i)
		if rv != lv {
			err = builder.StrAppend("1")
		} else {
			err = builder.StrAppend("0")
		}
		result.Length++
		if err != nil {
			panic("Expr Eval failed")
		}
	}
	result.Value = builder.Builder.NewArray()
	return result
}
