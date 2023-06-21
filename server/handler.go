package server

import (
	"github.com/go-mysql-org/go-mysql/mysql"
)

type SummerSQLHandler struct {
}

func (s SummerSQLHandler) UseDB(dbName string) error {
	//TODO implement me
	panic("implement me")
}

func (s SummerSQLHandler) HandleQuery(query string) (*mysql.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (s SummerSQLHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	//TODO implement me
	panic("implement me")
}

func (s SummerSQLHandler) HandleStmtPrepare(query string) (params int, columns int, context interface{}, err error) {
	//TODO implement me
	panic("implement me")
}

func (s SummerSQLHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (s SummerSQLHandler) HandleStmtClose(context interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (s SummerSQLHandler) HandleOtherCommand(cmd byte, data []byte) error {
	//TODO implement me
	panic("implement me")
}
