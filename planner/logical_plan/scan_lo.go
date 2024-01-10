package logical_plan

import (
	"bytes"
	"fmt"
)

type LogicalScan struct {
	TableName  string
	SchemaName string
}

func NewScan(tableName, schemaName string) (*LogicalScan, error) {
	return &LogicalScan{TableName: tableName, SchemaName: schemaName}, nil
}

func (s *LogicalScan) Build() error {
	return nil
}

func (s *LogicalScan) VisitSubOperator(closure Closure) error {
	return nil
}

func (s *LogicalScan) ToString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("\tScan: %s.%s;", s.SchemaName, s.TableName))

	//	if s.Projection != nil {
	//		buffer.WriteString(fmt.Sprintf(" projection=%s", s.Projection.ToString()))
	//	}
	//	if s.Quals != nil {
	//		buffer.WriteString(fmt.Sprintf(" filter=%s", s.Quals.ToString()))
	//	}
	return buffer.String()
}
