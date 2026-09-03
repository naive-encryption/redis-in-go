package main

import (
	"flag"
	"fmt"
	"net"
	"strconv"

	"redis-in-go/internal/commands"
	"redis-in-go/internal/info"
	"redis-in-go/internal/replica"
	"redis-in-go/internal/store"
)

func main() {
	port := flag.Int("port", 6379, "Port number")
	replicaOf := flag.String("replicaof", "", "Replica")

	flag.Parse()

	portProvidedParsed := ":" + strconv.Itoa(*port)

	store := store.NewStore()

	masterNode := commands.InitMasterNode()

	if *replicaOf == "" {
		info.SetRole("master")
	} else {
		info.SetRole("slave")
		go replica.ConnectToMaster(*replicaOf, *port, store)
	}

	info.MasterReplID = info.GenerateMasterReplID()

	ln, err := net.Listen("tcp", portProvidedParsed)
	if err != nil {
		panic(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go func(c net.Conn) {
			h := commands.InitHandler(conn, store, masterNode)
			h.HandleIncomingStream(conn)
		}(conn)
	}
}
