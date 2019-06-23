package main

import (
	"github.com/meateam/download-service/server"
)

func main() {
	server.NewServer(nil).Serve(nil)
}
