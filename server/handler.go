package server

import (
	"github.com/pingcap/tidb/parser"
	"summerSQL/optimizer"
	"summerSQL/planner"
	"summerSQL/portal"
	"summerSQL/server/mysql"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var SelectRowsResult = &sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "id",
			Type: querypb.Type_INT32,
		},
		{
			Name: "name",
			Type: querypb.Type_VARCHAR,
		},
	},
	Rows: [][]sqltypes.Value{
		{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("10")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name")),
		},
		{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("20")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nicer name")),
		},
	},
}

type SummerSQLHandler struct {
}

func NewSummerSQLHandler() *SummerSQLHandler {
	return &SummerSQLHandler{}
}

func (s SummerSQLHandler) NewConnection(c *mysql.Conn) {
	//TODO implement me
	//panic("implement me")
}

func (s SummerSQLHandler) ConnectionReady(c *mysql.Conn) {
	//TODO implement me
	//panic("implement me")
}

func (s SummerSQLHandler) ConnectionClosed(c *mysql.Conn) {
	//TODO implement me
	//	panic("implement me")
}

func (s SummerSQLHandler) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	//TODO: handle the errors and warnings
	//TODO: multi statements will be support further

	// parse query
	parser := parser.New()
	sn, _, _ := parser.Parse(query, "", "")
	// plan query
	logicalPlan := planner.CreateLogicalPlan(sn)
	// optimize query plan
	optimizedLogicalPlan, _ := optimizer.Optimize(logicalPlan)
	// create physical plan
	physicalPlan, _ := planner.CreatePhysicalPlan(optimizedLogicalPlan)
	// execute the plan
	finalResult := portal.ExecutePlan(physicalPlan)
	// echo back the result
	callback(&finalResult)

	return nil
}

func (s SummerSQLHandler) ComPrepare(c *mysql.Conn, query string, bindVars map[string]*querypb.BindVariable) ([]*querypb.Field, error) {
	//TODO implement me
	panic("implement me")
}

func (s SummerSQLHandler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	//TODO implement me
	panic("implement me")
}

func (s SummerSQLHandler) ComRegisterReplica(c *mysql.Conn, replicaHost string, replicaPort uint16, replicaUser string, replicaPassword string) error {
	//TODO implement me
	panic("implement me")
}

func (s SummerSQLHandler) ComBinlogDump(c *mysql.Conn, logFile string, binlogPos uint32) error {
	//TODO implement me
	panic("implement me")
}

func (s SummerSQLHandler) ComBinlogDumpGTID(c *mysql.Conn, logFile string, logPos uint64, gtidSet mysql.GTIDSet) error {
	//TODO implement me
	panic("implement me")
}

func (s SummerSQLHandler) WarningCount(c *mysql.Conn) uint16 {
	//TODO implement me
	//panic("implement me")
	return 0
}

func (s SummerSQLHandler) ComResetConnection(c *mysql.Conn) {
	//TODO implement me
	panic("implement me")
}
