package catalog

import (
	"errors"
	"github.com/apache/arrow/go/v11/arrow"
)

var (
	ColumnDefineMissing = errors.New("can't find column define")
)

type Schema = arrow.Schema
