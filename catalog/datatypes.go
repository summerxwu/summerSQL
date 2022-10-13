package catalog

import "errors"

const (
	IntTypeName    = "int"
	StringTypeName = "string"
	BoolTypeName   = "bool"
	DoubleTypeName = "double"
)

var (
	UnsupportedType = errors.New("unsupported type")
)

type IDataTypes interface {
	Name() string
}

type IntType struct {
	RealType interface{}
}

func (i IntType) Name() string {
	return IntTypeName
}

type StringType struct {
	RealType interface{}
}

func (s StringType) Name() string {
	return StringTypeName
}

type BoolType struct {
	RealType interface{}
}

func (b BoolType) Name() string {
	return BoolTypeName
}

type DoubleType struct {
	RealType interface{}
}

func (d DoubleType) Name() string {
	return DoubleTypeName
}
