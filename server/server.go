package server

import "summerSQL/server/mysql"

func StartServer(address string) error {
	handler := NewSummerSQLHandler()
	authServer := mysql.NewAuthServerNone()
	listener, err := mysql.NewListener("unix", address, authServer, handler, 0, 0, false, false)
	if err != nil {
		return err
	}
	//start listening
	listener.Accept()
	return nil
}
