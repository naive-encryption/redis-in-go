package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"

	"redis-in-go/internal/commands"
	"redis-in-go/internal/info"
	"redis-in-go/internal/store"
)

func main() {
	port := flag.Int("port", 6379, "Port number")
	replicaOf := flag.String("replicaof", "", "Replica")

	flag.Parse()

	portProvidedParsed := ":" + strconv.Itoa(*port)

	store := store.NewStore()

	if *replicaOf == "" {
		info.SetRole("master")
	} else {
		info.SetRole("slave")
		go connectToMaster(*replicaOf, *port, store)
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
		go func() {
			h := commands.InitHandler(conn, store)
			h.HandleIncomingStream(conn)
		}()
	}
}

func connectToMaster(replicaOf string, myPort int, store *store.Store) {
	parts := strings.Split(replicaOf, " ")
	masterAddr := parts[0] + ":" + parts[1]

	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		fmt.Println(err)
		return
	}

	reader := bufio.NewReader(conn)

	fmt.Fprint(conn, "*1\r\n$4\r\nPING\r\n")

	if err := readSimpleString(reader, "PONG"); err != nil {
		fmt.Println("handshake failed at PING:", err)
		return
	}

	replconfResponse1 := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%d\r\n", myPort)
	fmt.Fprint(conn, replconfResponse1)

	if err := readSimpleString(reader, "OK"); err != nil {
		fmt.Println("handshake failed at REPLCONF listening port:", err)
		return
	}

	replconfResponse2 := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	fmt.Fprint(conn, replconfResponse2)

	if err := readSimpleString(reader, "OK"); err != nil {
		fmt.Println("handshake failed at REPLCONF capa:", err)
		return
	}

	replicationID := "?"
	offset := -1
	psyncResponse := fmt.Sprintf("*3\r\n$5\r\nPSYNC\r\n$1\r\n%s\r\n$2\r\n%d\r\n", replicationID, offset)
	fmt.Fprint(conn, psyncResponse)
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func readSimpleString(r *bufio.Reader, expected string) error {
	line, err := readLine(r)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(line, "+") {
		return fmt.Errorf("expected simple string, found %s", line)
	}

	got := strings.TrimPrefix(line, "+")
	if got != expected {
		return fmt.Errorf("expected: %s, got: %s\n", expected, got)
	}
	return nil
}
