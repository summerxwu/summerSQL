package summerSQLparser

import (
	"fmt"
	"testing"
)

func TestParser(t *testing.T) {
	sql := "SELECT \n    SUM(IF(status != 200, 1, 0)) AS errors, \n    SUM(IF(status = 200, 1, 0)) AS success, \n    errors / COUNT(server) AS error_rate, \n    success / COUNT(server) AS success_rate, \n    SUM(response_time) / COUNT(server) AS load_avg, \n    MIN(response_time), \n    MAX(response_time), \n    path, \n    server\nFROM logmock \nGROUP BY \n    server, \n    path\nHAVING errors > 0\nORDER BY \n    server ASC, \n    load_avg DESC"
	sql = "SELECT a,b,c from table1 where a=1 and d = 2"
	sn, err := Parse(sql)
	if err != nil {
		t.Error(fmt.Sprint(err.Error()))
	}
	for i, s := range sn {

		println("%v %v", i, s)

	}
}
