package physical_plan

import (
	"bytes"
	"fmt"
	"github.com/apache/arrow/go/v6/arrow/array"
	"github.com/apache/arrow/go/v6/arrow/decimal128"
	"summerSQL/catalog"
	"summerSQL/datasource"
)

type ScanExec struct {
	DataSource datasource.IDataSource
	Projection *catalog.Schema
}

func NewScanExec(datasource datasource.CSVDataSource, projection *catalog.Schema) *ScanExec {
	return &ScanExec{}
}

func (s *ScanExec) ToString() string {
	schema := s.DataSource.Schema()
	return fmt.Sprintf(
		"ScanExec: schema: %s, projection: %s", schema.ToString(), s.Projection.ToString(),
	)
}

func (s *ScanExec) Schema() catalog.Schema {
	if len(s.Projection.Fields) == 0 {
		return s.DataSource.Schema()
	}
	return *(s.Projection)
}

func (s *ScanExec) Execute() *catalog.BatchColumns {
	rSlt, err := s.DataSource.Scan(*s.Projection)
	if err != nil {
		return nil
	}
	return &rSlt
}

func (s *ScanExec) ChildNodes() []IPhysicalPlan {
	return make([]IPhysicalPlan, 0)
}

type ProjectionExec struct {
	Input  IPhysicalPlan
	Expr   []IPhysicalExpr
	schema *catalog.Schema
}

func (p *ProjectionExec) Schema() catalog.Schema {
	return *p.schema
}

func (p *ProjectionExec) Execute() *catalog.BatchColumns {
	inputBatch := p.Input.Execute()
	outputBatch := &catalog.BatchColumns{
		BatchSchema: *p.schema,
		BatchVector: make([]catalog.IColumnVector, 0),
	}
	for _, expr := range p.Expr {
		cv := expr.Evaluate(*inputBatch)
		outputBatch.BatchVector = append(outputBatch.BatchVector, cv)
	}
	return outputBatch
}

func (p *ProjectionExec) ChildNodes() []IPhysicalPlan {
	r := make([]IPhysicalPlan, 0)
	r = append(r, p.Input)
	return r
}

func (p *ProjectionExec) ToString() string {
	buff := bytes.Buffer{}
	buff.WriteString("ProjectionExec: ")
	for i, expr := range p.Expr {
		buff.WriteString(fmt.Sprintf(" #%d#", i))
		buff.WriteString(expr.ToString())
	}
	return buff.String()
}

type FilterExec struct {
	Input IPhysicalPlan
	Expr  IPhysicalExpr
}

func (f *FilterExec) Schema() catalog.Schema {
	return f.Input.Schema()
}

func (f *FilterExec) Execute() *catalog.BatchColumns {
	bc := f.Input.Execute()
	rcv := f.Expr.Evaluate(*bc)
	// based on rcv(result column vector) to rebuild the final batch column
	result := catalog.BatchColumns{
		BatchSchema: bc.BatchSchema,
		BatchVector: make([]catalog.IColumnVector, 0),
	}
	for _, vector := range bc.BatchVector {
		t := f.filterColumnVec(vector, rcv)
		result.BatchVector = append(result.BatchVector, t)
	}
	return &result
}

func (f *FilterExec) ChildNodes() []IPhysicalPlan {
	r := make([]IPhysicalPlan, 0)
	r = append(r, f.Input)
	return r
}

func (f *FilterExec) ToString() string {
	return ""
}

func (f *FilterExec) filterColumnVec(
	input catalog.IColumnVector, filterVec catalog.IColumnVector,
) catalog.IColumnVector {
	// TODO: ArrowColumnVector is the only default impl for now, we need to abstract the filter logic func
	inputValue := input.(*catalog.ArrowColumnVector)
	filterValue := filterVec.(*catalog.ArrowColumnVector)
	columnDef := inputValue.ColumnSpec
	result := catalog.NewArrowColumnVector(&columnDef)
	builder := catalog.NewArrowColumnVectorBuilder(&columnDef)

	for i := 0; i < filterValue.Size(); i++ {
		swapOut := filterValue.GetValue(i).(bool)
		if swapOut != true {
			switch v := builder.Builder.(type) {
			case *array.StringBuilder:
				{
					v.Append(inputValue.GetValue(i).(string))
					break
				}
			case *array.Int64Builder:
				{
					v.Append(inputValue.GetValue(i).(int64))
					break
				}
			case *array.BooleanBuilder:
				{
					v.Append(inputValue.GetValue(i).(bool))
					break
				}
			case *array.Decimal128Builder:
				{
					v.Append(inputValue.GetValue(i).(decimal128.Num))
					break
				}
			default:
				panic("data type not support")
			}

		}
		continue
	}
	result.Value = builder.Builder.NewArray()
	return result
}

// AggregateExec is only as Hash manner for now
// TODO: support more aggregate play strategy
type AggregateExec struct {
	Input     IPhysicalPlan
	GroupExpr []IPhysicalExpr
	AggExpr   []IAggregatePhysicalExpr
	schema    catalog.Schema
}

func (a *AggregateExec) Schema() catalog.Schema {
	return a.schema
}

func serialize(input []any) string {
	str := ""
	for _, a := range input {
		str += fmt.Sprint(a)
	}
	return str
}

func (a *AggregateExec) Execute() *catalog.BatchColumns {
	// Get the BatchColumn from the input physical expression's evaluation
	inputBatchColumns := a.Input.Execute()
	// Get the GroupExpr result
	groupExprResult := make([]catalog.IColumnVector, 0)
	// preAggExprResult keep the input column vector of the aggregate expression
	preAggExprResult := make([]catalog.IColumnVector, 0)

	for _, expr := range a.GroupExpr {
		groupExprResult = append(groupExprResult, expr.Evaluate(*inputBatchColumns))
	}
	for _, expr := range a.AggExpr {
		preAggExprResult = append(preAggExprResult, expr.InputIs().Evaluate(*inputBatchColumns))
	}

	// we need an accumulator map which indexed by the group identical value
	rowAccumulatorMap := make(map[string][]IAccumulator)

	// filling the rowAccumulatorMap
	for i := 0; i < inputBatchColumns.RowCount(); i++ {
		groupColumnRowBuffer := make([]interface{}, 0)
		for _, vector := range groupExprResult {
			groupColumnRowBuffer = append(groupColumnRowBuffer, vector.GetValue(i))
		}
		// serialize the row buffer as the rowAccumulatorMap key
		md5 := serialize(groupColumnRowBuffer)

		preAggRowBuff := make([]interface{}, 0)
		for _, vector := range preAggExprResult {
			preAggRowBuff = append(preAggRowBuff, vector.GetValue(i))
		}
		// Get the accumulator slice if exists
		accVec, found := rowAccumulatorMap[md5]
		if found == true {
			for i2, accumulator := range accVec {
				accumulator.Accumulate(preAggRowBuff[i2])
			}

		} else {
			// create accumulator and accumulate current row
			newAccVec := make([]IAccumulator, 0)
			for i3, expr := range a.AggExpr {
				t := expr.CreateAccumulator()
				newAccVec = append(newAccVec, t)
				t.Accumulate(preAggRowBuff[i3])
			}
			rowAccumulatorMap[md5] = newAccVec
		}
	}

	// generate the final BatchColumn based on the rowAccumulatorMap
	return nil
}

func (a AggregateExec) ChildNodes() []IPhysicalPlan {
	rt := make([]IPhysicalPlan, 0)
	rt = append(rt, a.Input)
	return rt
}

func (a AggregateExec) ToString() string {
	return ""
}
