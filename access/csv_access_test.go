package access

import (
	"fmt"
	"testing"
)

func TestCsvDataSource(t *testing.T) {
	// inputCsvFileFullPath := "/Users/summerxwu/GolandProjects/summerSQL/test_data/employee.csv"
	inputCsvFileFullPath := "/Users/summerxwu1/GolandProjects/summerSQL/test_data/2018_Yellow_Taxi_Trip_Data.csv"
	inputCsvFileFullPath = "/Users/summerxwu1/GolandProjects/summerSQL/test_data/employee.csv"

	dataSource, err := NewInferringCSVDataSource(inputCsvFileFullPath, 10)
	if dataSource == nil {
		t.Fatalf("New Datasource failed : %s", err.Error())
	}
	for true {
		retVal, err := dataSource.Inhale()
		if err != nil {
			t.Fatalf("%s", err.Error())
			return
		}
		if retVal == nil {
			return
		}
		//fmt.Println(retVal.Schema().String())
		//return
		// print the value
		for i := 0; i < int(retVal.NumCols()); i++ {
			// print column i
			fmt.Println(retVal.Column(i).String())
			//break
		}

	}
}
