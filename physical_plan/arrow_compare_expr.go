package physical_plan

import (
	"summerSQL/catalog"
)

func ArrowEqBinaryFunc(l *catalog.ArrowColumnVector, r *catalog.ArrowColumnVector) *catalog.ArrowColumnVector {
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

func ArrowNeqBinaryFunc(l *catalog.ArrowColumnVector, r *catalog.ArrowColumnVector) *catalog.ArrowColumnVector {
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
