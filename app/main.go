package main

import (
	"bufio"
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
	}

	info.MasterReplID = info.GenerateMasterReplID()

	ln, err := net.Listen("tcp", portProvidedParsed)
	if err != nil {
		panic(err)
	}

	if *replicaOf != "" {
		go replica.ConnectToMaster(*replicaOf, *port, store)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		reader := bufio.NewReader(conn)
		go func(c net.Conn, r *bufio.Reader) {
			isMasterConn := false
			h := commands.InitHandler(c, store, masterNode, isMasterConn)
			h.HandleIncomingStream(c, r)
		}(conn, reader)
	}
}
