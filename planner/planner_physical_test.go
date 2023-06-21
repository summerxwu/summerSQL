package planner

import (
	"fmt"
	"summerSQL/catalog"
	logical_plan2 "summerSQL/planner/logical_plan"
	"testing"
)

func TestCreateAndExecutePhysicalPlan(t *testing.T) {
	inputCsvFileFullPath := "/Users/summerxwu/GolandProjects/summerSQL/test_data/employee.csv"
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
	eCtx, err := logical_plan2.ExecutionContext{}.CSVDataFrame(inputCsvFileFullPath, inputCsvSchema)
	if err != nil {
		t.Fatalf(err.Error())
	}
	eCtx.Filter(
		logical_plan2.NewEq(
			&logical_plan2.ColumnExpr{Name: "state"}, &logical_plan2.LiteralStringExpr{
				Literal: "CO",
			},
		),
	).Projection(
		[]logical_plan2.ILogicExpr{
			&logical_plan2.ColumnExpr{
				Name: "id",
			},
			&logical_plan2.ColumnExpr{
				Name: "fisrt_name",
			},
			&logical_plan2.ColumnExpr{
				Name: "last_name",
			},
		},
	)
	fmt.Printf(logical_plan2.PrintPretty(eCtx.Final_logicPlan, "", "    "))

	ph, err := CreatePhysicalPlan(eCtx.Final_logicPlan)
	if err != nil {
		t.Fatalf(err.Error())
	}

	br := ph.Execute()
	br.Print()
}
