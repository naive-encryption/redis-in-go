package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"strconv"

	"redis-in-go/internal/aof"
	"redis-in-go/internal/commands"
	"redis-in-go/internal/info"
	rdbparser "redis-in-go/internal/rdbParser"
	"redis-in-go/internal/replica"
	"redis-in-go/internal/store"
)

func main() {
	port := flag.Int("port", 6379, "Port number")
	replicaOf := flag.String("replicaof", "", "Replica")

	rdbDir := flag.String("dir", "", "Working directory")
	rdbFileName := flag.String("dbfilename", "", "RDB file name")

	aofAppendOnly := flag.String("appendonly", "no", "")
	aofAppendDirName := flag.String("appenddirname", "appendonlydir", "")
	aofAppendFileName := flag.String("appendfilename", "appendonly.aof", "")
	aofAppendFSync := flag.String("appendfsync", "everysec", "")

	flag.Parse()

	initRDBInfo(*rdbDir, *rdbFileName)
	aof.Init(*aofAppendOnly, *aofAppendDirName, *aofAppendFileName, *aofAppendFSync)

	portProvidedParsed := ":" + strconv.Itoa(*port)

	store := store.NewStore()
	loadRDBIntoStore(info.WorkDir+"/"+info.RDBFileName, store)

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

func initRDBInfo(rdbDir, rdbFileName string) {
	if rdbDir != "" {
		info.WorkDir = rdbDir
	}
	if rdbFileName != "" {
		info.RDBFileName = rdbFileName
	}
}

func loadRDBIntoStore(path string, s *store.Store) error {
	entries, err := rdbparser.ReadRDBFile(path)
	if err != nil {
		return err
	}

	for _, e := range entries {
		s.SetWithAbsoluteExpiry(e.Key, e.Value, e.ExpireAt)
	}
	return nil
}
