package main

import (
	"flag"
	"fmt"
	"net"
	"strconv"

	"redis-in-go/internal/commands"
	"redis-in-go/internal/store"
)

func main() {
	port := flag.Int("port", 6379, "Port number")

	flag.Parse()

	portProvidedParsed := ":" + strconv.Itoa(*port)

	ln, err := net.Listen("tcp", portProvidedParsed)
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
