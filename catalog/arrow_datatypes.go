package catalog

import "github.com/apache/arrow/go/v6/arrow"

func NewArrowIntType() IDataTypes {
	return &IntType{RealType: arrow.Int64Type{}}
}

func NewArrowStringType() IDataTypes {
	return &StringType{RealType: arrow.StringType{}}
}

func NewArrowBoolType() IDataTypes {
	return &BoolType{RealType: arrow.BooleanType{}}
}

func NewArrowDoubleType() IDataTypes {
	return &DoubleType{RealType: arrow.Decimal128Type{}}
}
