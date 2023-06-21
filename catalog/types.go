package catalog

import (
	"errors"
	"github.com/apache/arrow/go/v11/arrow"
)

var (
	UnsupportedType = errors.New("unsupported type")
)

type IDataTypes = arrow.DataType
