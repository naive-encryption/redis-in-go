package main

import (
	"fmt"
	"net"

	"redis-in-go/internal/commands"
	"redis-in-go/internal/store"
)

func main() {
	ln, err := net.Listen("tcp", ":6379")
	if err != nil {
		panic(err)
	}

	store := store.NewStore()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go func() {
			h := commands.NewHandler(conn, store)
			h.HandleIncomingStream(conn)
		}()
	}
}
