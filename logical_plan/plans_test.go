package logical_plan

import (
	"fmt"
	"summerSQL/catalog"
	"summerSQL/datasource"
	"testing"
)

func TestNewLogicPlanAndPrint(t *testing.T) {

	// SELECT * FROM employee WHERE state = 'CO'

	// construct scan
	inputCsvFileFullPath := "/Users/summerxwu/GolandProjects/summerSQL/test/employee.csv"
	inputCsvSchema := catalog.Schema{
		Fields: make([]*catalog.Column, 0),
	}
	// id,first_name,last_name,state,job_title,salary,insurance
	// 1,Bill,Hopkins,CA,Manager,12000,true
	// 2,Gregg,Langford,CO,Driver,10000,false
	// 3,John,Travis,CO,"Manager, Software",11500,true
	// 4,Von,Mill,,Defensive End,11500,false
	cId := catalog.NewColumn(catalog.NewArrowIntType(), "id", 0)
	inputCsvSchema.Fields = append(inputCsvSchema.Fields, cId)
	cFirstName := catalog.NewColumn(catalog.NewArrowStringType(), "fisrt_name", 1)
	inputCsvSchema.Fields = append(inputCsvSchema.Fields, cFirstName)
	cLastName := catalog.NewColumn(catalog.NewArrowStringType(), "last_name", 2)
	inputCsvSchema.Fields = append(inputCsvSchema.Fields, cLastName)
	cState := catalog.NewColumn(catalog.NewArrowStringType(), "state", 3)
	inputCsvSchema.Fields = append(inputCsvSchema.Fields, cState)
	cJobTitle := catalog.NewColumn(catalog.NewArrowStringType(), "job_title", 4)
	inputCsvSchema.Fields = append(inputCsvSchema.Fields, cJobTitle)
	cSalary := catalog.NewColumn(catalog.NewArrowDoubleType(), "salary", 5)
	inputCsvSchema.Fields = append(inputCsvSchema.Fields, cSalary)
	cInsurance := catalog.NewColumn(catalog.NewArrowBoolType(), "insurance", 6)
	inputCsvSchema.Fields = append(inputCsvSchema.Fields, cInsurance)

	// projection := catalog.Schema{
	//	Fields: make([]*catalog.Column, 0),
	// }
	// projection.Fields = append(projection.Fields, cId)
	// projection.Fields = append(projection.Fields, cFirstName)
	// projection.Fields = append(projection.Fields, cJobTitle)
	// projection.Fields = append(projection.Fields, cInsurance)
	ds := datasource.NewCSVDataSource(inputCsvFileFullPath, inputCsvSchema, 100)

	scan, _ := NewScan(ds, nil)
	// construct Filter
	// setup Expr
	clExpr := ColumnExpr{
		Name: "state",
	}
	lStrExpr := LiteralStringExpr{
		Literal: "CO",
	}
	eq := NewEq(&clExpr, &lStrExpr)
	Expr := make([]ILogicExpr, 0)
	Expr = append(Expr, eq)
	filter := NewFilter(scan, Expr)
	// construct projection
	// construct column expr
	CExpr := make([]ILogicExpr, 0)
	C1 := &ColumnExpr{
		Name: "id",
	}
	CExpr = append(CExpr, C1)
	C2 := &ColumnExpr{
		Name: "first_name",
	}
	CExpr = append(CExpr, C2)
	C3 := &ColumnExpr{
		Name: "last_name",
	}
	CExpr = append(CExpr, C3)
	C4 := &ColumnExpr{
		Name: "state",
	}
	CExpr = append(CExpr, C4)
	C5 := &ColumnExpr{
		Name: "salary",
	}
	CExpr = append(CExpr, C5)

	pj := NewProjection(filter, CExpr)
	fmt.Println(PrintPretty(pj, "", "    "))

}
