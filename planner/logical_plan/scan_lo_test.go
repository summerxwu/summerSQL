package logical_plan

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewScan(t *testing.T) {
	scan, err := NewScan("test_table", "test_schema")
	assert.Nil(t, err)
	err = scan.Build()
	assert.Nil(t, err)
	fmt.Println(PrintPretty(scan))
}
