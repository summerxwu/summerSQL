package logical_plan

import (
	"bytes"
	"errors"
	"fmt"
	"summerSQL/catalog"
)

type Scan struct {
	Projection *Projector      `json:"projection"`
	RelSchema  *catalog.Schema `json:"rel_schema"`
	RelName    string          `json:"rel_name,omitempty"`
	Quals      *Filter
}

func NewScan(relSchema *catalog.Schema, relName string) (*Scan, error) {
	if relSchema == nil {
		return nil, errors.New(fmt.Sprintf("Invalid schema define for %s", relName))
	}

	sc := &Scan{Projection: nil, RelSchema: relSchema, RelName: relName, Quals: nil}
	return sc, nil
}

func (s *Scan) AddProjectionOnScan(projection *Projector) {
	s.Projection = projection
}

func (s *Scan) AddFilterOnScan(filter *Filter) {
	s.Quals = filter
}

func (s *Scan) Schema() *catalog.Schema {
	if s.Projection != nil {
		return s.Projection.Schema()
	}
	return s.RelSchema
}

func (s *Scan) Children() []ILogicPlan {
	return nil
}

func (s *Scan) ToString() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("Scan: %s;", s.RelName))

	if s.Projection != nil {
		buffer.WriteString(fmt.Sprintf(" projection=%s", s.Projection.ToString()))
	}
	if s.Quals != nil {
		buffer.WriteString(fmt.Sprintf(" filter=%s", s.Quals.ToString()))
	}
	return buffer.String()
}
