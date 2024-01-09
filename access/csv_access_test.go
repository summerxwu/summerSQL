package access

import (
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"summerSQL/catalog"
	"summerSQL/util"
	"testing"
)

func createTTDSchema() *catalog.TSchema {
	fields := make([]catalog.TField, 0)
	//VendorID,
	fields = append(fields, catalog.TField{Name: "VendorID", Type: &arrow.Int8Type{}, Nullable: false})
	//tpep_pickup_datetime,
	fields = append(fields, catalog.TField{Name: "tpep_pickup_datetime", Type: &arrow.StringType{}, Nullable: false})
	//tpep_dropoff_datetime,
	fields = append(fields, catalog.TField{Name: "tpep_dropoff_datetime", Type: &arrow.StringType{}, Nullable: false})
	//passenger_count,
	fields = append(fields, catalog.TField{Name: "passenger_count", Type: &arrow.Int8Type{}, Nullable: false})
	//trip_distance,
	fields = append(fields, catalog.TField{Name: "trip_distance", Type: &arrow.Float32Type{}, Nullable: false})
	//RatecodeID,
	fields = append(fields, catalog.TField{Name: "RatecodeID", Type: &arrow.Int8Type{}, Nullable: false})
	//store_and_fwd_flag,
	fields = append(fields, catalog.TField{Name: "store_and_fwd_flag", Type: &arrow.StringType{}, Nullable: false})
	//PULocationID,
	fields = append(fields, catalog.TField{Name: "PULocationID", Type: &arrow.Int16Type{}, Nullable: false})
	//DOLocationID,
	fields = append(fields, catalog.TField{Name: "DOLocationID", Type: &arrow.Int16Type{}, Nullable: false})
	//payment_type,
	fields = append(fields, catalog.TField{Name: "payment_type", Type: &arrow.Int16Type{}, Nullable: false})
	//fare_amount,
	fields = append(fields, catalog.TField{Name: "fare_amount", Type: &arrow.Float32Type{}, Nullable: false})
	//extra,
	fields = append(fields, catalog.TField{Name: "extra", Type: &arrow.Float32Type{}, Nullable: false})
	//mta_tax,
	fields = append(fields, catalog.TField{Name: "mta_tax", Type: &arrow.Float32Type{}, Nullable: false})
	//tip_amount,
	fields = append(fields, catalog.TField{Name: "tip_amount", Type: &arrow.Float32Type{}, Nullable: false})
	//tolls_amount,
	fields = append(fields, catalog.TField{Name: "tolls_amount", Type: &arrow.Float32Type{}, Nullable: false})
	//improvement_surcharge,
	fields = append(fields, catalog.TField{Name: "improvement_surcharge", Type: &arrow.Float32Type{}, Nullable: false})
	//total_amount
	fields = append(fields, catalog.TField{Name: "total_amount", Type: &arrow.Float32Type{}, Nullable: false})

	return arrow.NewSchema(fields, nil)
}

func TestCsvDataSource(t *testing.T) {
	// inputCsvFileFullPath := "/Users/summerxwu/GolandProjects/summerSQL/test_data/employee.csv"
	inputCsvFileFullPath := "/Users/summerxwu1/GolandProjects/summerSQL/test_data/2018_Yellow_Taxi_Trip_Data.csv"
	//inputCsvFileFullPath = "/Users/summerxwu1/GolandProjects/summerSQL/test_data/employee.csv"

	//dataSource, err := NewInferringCSVDataSource(inputCsvFileFullPath, 10)
	dataSource, err := NewCSVDataSource(inputCsvFileFullPath, createTTDSchema(), 100)
	if dataSource == nil {
		t.Fatalf("New Datasource failed : %s", err.Error())
	}
	for {
		retVal, err := dataSource.Inhale()
		if err != nil {
			var err_ *util.Error
			if errors.As(err, &err_) && err_.Code() == util.BatchSizeExhausted {
				return
			}
			t.Fatalf("%s", err.Error())
			return
		}
		if retVal == nil {
			return
		}
		for i := 0; i < int(retVal.NumRows()); i++ {
			for j := 0; j < int(retVal.NumCols()); j++ {
				column := retVal.Column(j)
				switch column.DataType().ID() {
				case arrow.STRING:
					{
						fmt.Printf("%s, ", column.(*array.String).Value(i))
						break
					}
				case arrow.INT8:
					{
						fmt.Printf("%d, ", column.(*array.Int8).Value(i))
						break
					}
				case arrow.INT16:
					{
						fmt.Printf("%d, ", column.(*array.Int16).Value(i))
						break
					}
				case arrow.INT32:
					{
						fmt.Printf("%d, ", column.(*array.Int32).Value(i))
						break
					}
				case arrow.INT64:
					{
						fmt.Printf("%d, ", column.(*array.Int64).Value(i))
						break
					}
				case arrow.BOOL:
					{
						fmt.Printf("%v, ", column.(*array.Boolean).Value(i))
						break
					}
				case arrow.FLOAT64:
					{
						fmt.Printf("%f, ", column.(*array.Float64).Value(i))
					}
				case arrow.FLOAT32:
					{
						fmt.Printf("%f, ", column.(*array.Float32).Value(i))
					}
				default:
					panic("unhandled default case")
				}

			}
			fmt.Printf("\n")
		}

	}
}
