package executor

import (
	"fmt"
	"summerSQL/access"
	"summerSQL/catalog"
	"testing"
)

func createBatchColumn(path string) catalog.BatchColumns {
	inputCsvFileFullPath := path
	inputCsvSchema := catalog.TSchema{
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

	// init reader projection
	projection := catalog.TSchema{
		Fields: make([]*catalog.Column, 0),
	}
	projection.Fields = append(projection.Fields, cId)
	projection.Fields = append(projection.Fields, cFirstName)
	projection.Fields = append(projection.Fields, cJobTitle)
	projection.Fields = append(projection.Fields, cInsurance)

	dataSource := access.NewCSVDataSource(inputCsvFileFullPath, inputCsvSchema, 100)
	retVal, _ := dataSource.Scan(inputCsvSchema)
	retVal.Print()
	return retVal
}

func TestColumnPhysicalExpr_Evaluate(t *testing.T) {
	bc := createBatchColumn("/Users/summerxwu/GolandProjects/summerSQL/test_data/employee.csv")
	cExpr := NewColumnPhysicalExpr(2)
	rt := cExpr.Evaluate(bc)
	rtStr, err := rt.Print()
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(rtStr)
}

func TestLiteralStrPhysicalExpr_Evaluate(t *testing.T) {
	bc := createBatchColumn("/Users/summerxwu/GolandProjects/summerSQL/test_data/employee.csv")
	expr := NewLiteralStrPhysicalExpr("summer")
	rt := expr.Evaluate(bc)
	rtStr, err := rt.Print()
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(rtStr)

}

func TestLiteralBooleanPhysicalExpr_Evaluate(t *testing.T) {
	bc := createBatchColumn("/Users/summerxwu/GolandProjects/summerSQL/test_data/employee.csv")
	expr := NewLiteralBooleanPhysicalExpr("1")
	rt := expr.Evaluate(bc)
	rtStr, err := rt.Print()
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(rtStr)
}

func TestLiteralDoublePhysicalExpr_Evaluate(t *testing.T) {
	bc := createBatchColumn("/Users/summerxwu/GolandProjects/summerSQL/test_data/employee.csv")
	expr := NewLiteralDoublePhysicalExpr("1300")
	rt := expr.Evaluate(bc)
	rtStr, err := rt.Print()
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(rtStr)
}

func TestNewAddPhysicalExpr(t *testing.T) {
	bc := createBatchColumn("/Users/summerxwu/GolandProjects/summerSQL/test_data/employee.csv")
	expr := NewAddPhysicalExpr(NewColumnPhysicalExpr(0), NewColumnPhysicalExpr(0))
	rt := expr.Evaluate(bc)
	rtStr, err := rt.Print()
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(rtStr)
}

func TestNewEqBinaryPhysicalExpr(t *testing.T) {
	bc := createBatchColumn("/Users/summerxwu/GolandProjects/summerSQL/test_data/employee.csv")
	cExprL := NewColumnPhysicalExpr(2)
	cExprR := NewColumnPhysicalExpr(3)
	// expr := NewNeqBinaryPhysicalExpr(cExprL, cExprR)
	expr := NewEqBinaryPhysicalExpr(cExprL, cExprR)
	rt := expr.Evaluate(bc)
	rtStr, err := rt.Print()
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(rtStr)

}
