package catalog

import (
	"bytes"
	"errors"
)

type Schema struct {
	Fields []*Column
}

var (
	ColumnDefineMissing = errors.New("can't find column define")
)

func (s *Schema) Project(projection []string) (*Schema, error) {
	if len(projection) == 0 {
		return nil, errors.New("no projection provided")

	}
	ns := Schema{Fields: make([]*Column, 0)}
	for _, s2 := range projection {
		for i2, field := range s.Fields {
			if s2 == field.Name {
				ns.Fields = append(ns.Fields, field)
				break
			}
			if i2 == len(projection)-1 {
				return nil, ColumnDefineMissing
			}
		}
	}
	return &ns, nil
}

func (s *Schema) GetIndexByColumn(in *Column) (int, error) {
	for index, column := range s.Fields {
		if in.Name == column.Name {
			return index, nil
		}
	}
	return -1, ColumnDefineMissing
}

func (s *Schema) ToString() string {
	buffer := bytes.Buffer{}
	buffer.WriteString("{")
	for i, field := range s.Fields {
		if i == len(s.Fields)-1 {
			buffer.WriteString(field.Name)
		} else {

			buffer.WriteString(field.Name)
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString("}")
	return buffer.String()
}
