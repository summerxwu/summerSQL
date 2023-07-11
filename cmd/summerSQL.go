package main

import (
	"flag"
	"fmt"
	"os"
	"summerSQL/server"
)

func main() {
	flag.Parse()
	os.RemoveAll("./summerSQL.sock")
	fmt.Println("Server starting...")
	server.StartServer("./summerSQL.sock")
}
