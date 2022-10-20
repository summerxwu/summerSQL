package logical_plan

import (
	"bytes"
	"fmt"
	"summerSQL/catalog"
)

// ILogicPlan (e.g. The relation algebra operator) represent a
// relation which consist of tuples with the relation schema
type ILogicPlan interface {
	Schema() *catalog.Schema
	// Children return the Input logic plan
	Children() []ILogicPlan
	// ToString return the brief description about current logic plan
	ToString() string
}

func PrintPretty(plan ILogicPlan, prefix string, indent string) string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%s%s", prefix, indent))
	buffer.WriteString(fmt.Sprintf("%s\n", plan.ToString()))
	if len(plan.Children()) == 0 {
		return buffer.String()
	}
	for _, logicPlan := range plan.Children() {
		childStr := PrintPretty(logicPlan, fmt.Sprintf("%s%s", prefix, indent), indent)
		buffer.WriteString(childStr)
	}
	return buffer.String()
}
