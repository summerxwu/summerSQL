package server

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/server"
	"net"
)

func ServerStart(ip string, port uint64) error {
	// Listen for connections on localhost port 4000
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return err
	}

	// Accept a new connection once
	c, err := l.Accept()
	if err != nil {
		return err
	}

	// Create a connection with user root and an empty password.
	// You can use your own handler to handle command here.
	conn, err := server.NewConn(c, "root", "", server.EmptyHandler{})
	if err != nil {
		return err
	}

	// as long as the client keeps sending commands, keep handling them
	for {
		if err := conn.HandleCommand(); err != nil {
			return err
		}
	}
}
