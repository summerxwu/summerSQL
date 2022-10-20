package logical_plan

import (
	"bytes"
	"errors"
	"fmt"
	"summerSQL/catalog"
	"summerSQL/datasource"
)

type Scan struct {
	DataSource datasource.IDataSource
	Projection catalog.Schema
}

func NewScan(datasource datasource.IDataSource, projection *catalog.Schema) (*Scan, error) {
	if datasource == nil {
		return nil, errors.New(fmt.Sprintf("datasource is invalid"))
	}
	var pg *catalog.Schema
	if projection != nil {
		pg = projection
	} else {
		pg = &catalog.Schema{Fields: make([]*catalog.Column, 0)}
	}

	sc := &Scan{
		DataSource: datasource,
		Projection: *pg,
	}
	return sc, nil
}

func (s *Scan) Schema() *catalog.Schema {
	if len(s.Projection.Fields) == 0 {
		t := s.DataSource.Schema()
		return &t
	}
	return &(s.Projection)
}

func (s *Scan) Children() []ILogicPlan {
	return nil
}

func (s *Scan) ToString() string {
	if len(s.Projection.Fields) == 0 {
		return fmt.Sprintf("Scan: %s; projection=None", s.DataSource.Info())
	} else {
		return fmt.Sprintf("Scan: %s; projection=%s", s.DataSource.Info(), s.Projection.ToString())
	}
}

type Projection struct {
	ChildNodes ILogicPlan
	Expr       []ILogicExpr
}

func NewProjection(childNodes ILogicPlan, expr []ILogicExpr) *Projection {
	return &Projection{ChildNodes: childNodes, Expr: expr}
}

func (p *Projection) Schema() *catalog.Schema {
	sc := catalog.Schema{
		Fields: make([]*catalog.Column, 0),
	}
	for i, expr := range p.Expr {
		sc.Fields = append(sc.Fields, catalog.NewColumn(expr.ReturnType(p.ChildNodes), "", i))
	}
	return &sc
}

func (p *Projection) Children() []ILogicPlan {
	cd := make([]ILogicPlan, 0)
	cd = append(cd, p.ChildNodes)
	return cd
}

func (p *Projection) ToString() string {
	buff := bytes.Buffer{}
	buff.WriteString("Projection: ")
	for _, expr := range p.Expr {
		buff.WriteString(expr.ToString())
		buff.WriteString(", ")
	}
	return buff.String()
}

type Filter struct {
	ChildNodes ILogicPlan
	Expr       []ILogicExpr
}

func NewFilter(childNodes ILogicPlan, expr []ILogicExpr) *Filter {
	return &Filter{ChildNodes: childNodes, Expr: expr}
}

func (f *Filter) Schema() *catalog.Schema {
	return f.ChildNodes.Schema()
}

func (f *Filter) Children() []ILogicPlan {
	cd := make([]ILogicPlan, 0)
	cd = append(cd, f.ChildNodes)
	return cd
}

func (f *Filter) ToString() string {
	buff := bytes.Buffer{}
	buff.WriteString("Filter: ")
	for _, expr := range f.Expr {
		buff.WriteString(expr.ToString())
		buff.WriteString(" ")
	}
	return buff.String()
}

type Aggregate struct {
	ChildNodes ILogicPlan
	Expr       []ILogicExpr
	GroupExpr  []ILogicExpr
}

func NewAggregate(childNodes ILogicPlan, expr []ILogicExpr, groupExpr []ILogicExpr) *Aggregate {
	return &Aggregate{ChildNodes: childNodes, Expr: expr, GroupExpr: groupExpr}
}

func (a *Aggregate) Schema() *catalog.Schema {
	sc := catalog.Schema{
		Fields: make([]*catalog.Column, 0),
	}
	for i, expr := range a.Expr {
		sc.Fields = append(sc.Fields, catalog.NewColumn(expr.ReturnType(a.ChildNodes), "", i))
	}
	return &sc

}

func (a *Aggregate) Children() []ILogicPlan {
	cd := make([]ILogicPlan, 0)
	cd = append(cd, a.ChildNodes)
	return cd
}

func (a *Aggregate) ToString() string {
	buff := bytes.Buffer{}
	buff.WriteString("Aggregate:\n")
	for _, expr := range a.Expr {
		buff.WriteString("  agrgt=> ")
		buff.WriteString(expr.ToString())
		buff.WriteString("\n")
	}
	for _, expr := range a.GroupExpr {
		buff.WriteString("  group=> ")
		buff.WriteString(expr.ToString())
		buff.WriteString("\n")
	}
	return buff.String()
}
