package logical_plan

import (
	"fmt"
	"summerSQL/catalog"
	"testing"
)

func TestNewExecutionContext(t *testing.T) {
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
	eCtx, err := ExecutionContext{}.CSVDataFrame(inputCsvFileFullPath, inputCsvSchema)
	if err != nil {
		t.Fatalf(err.Error())
	}
	eCtx.Filter(
		[]ILogicExpr{
			NewEq(
				&ColumnExpr{"state"}, &LiteralStringExpr{
					Literal: "CO",
				},
			),
		},
	).Projection(
		[]ILogicExpr{
			&ColumnExpr{
				Name: "id",
			},
			&ColumnExpr{
				Name: "fisrt_name",
			},
			&ColumnExpr{
				Name: "last_name",
			},
		},
	)
	fmt.Printf(PrintPretty(eCtx.logicPlan, "", "    "))

}
