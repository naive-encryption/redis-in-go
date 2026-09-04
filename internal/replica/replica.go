package replica

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"redis-in-go/internal/commands"
	"redis-in-go/internal/store"
)

type Server struct {
	MasterConn net.Conn
	Store      *store.Store
	Mu         sync.RWMutex
}

func ConnectToMaster(replicaOf string, myPort int, store *store.Store) {
	parts := strings.Split(replicaOf, " ")
	masterAddr := parts[0] + ":" + parts[1]

	Conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		fmt.Println(err)
		return
	}

	reader := bufio.NewReader(Conn)

	fmt.Fprint(Conn, "*1\r\n$4\r\nPING\r\n")

	if err := ReadSimpleString(reader, "PONG"); err != nil {
		fmt.Println("handshake failed at PING:", err)
		return
	}

	replconfResponse1 := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%d\r\n", myPort)
	fmt.Fprint(Conn, replconfResponse1)

	if err := ReadSimpleString(reader, "OK"); err != nil {
		fmt.Println("handshake failed at REPLCONF listening port:", err)
		return
	}

	replconfResponse2 := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	fmt.Fprint(Conn, replconfResponse2)

	if err := ReadSimpleString(reader, "OK"); err != nil {
		fmt.Println("handshake failed at REPLCONF capa:", err)
		return
	}

	replicationID := "?"
	offset := -1
	psyncResponse := fmt.Sprintf("*3\r\n$5\r\nPSYNC\r\n$1\r\n%s\r\n$2\r\n%d\r\n", replicationID, offset)
	fmt.Fprint(Conn, psyncResponse)

	err = DiscardRDBPayload(reader)
	if err != nil {
		fmt.Println(err)
		return
	}

	isMasterConn := true
	h := commands.InitHandler(Conn, store, nil, isMasterConn)
	h.HandleIncomingStream(Conn, reader)
}

func DiscardRDBPayload(reader *bufio.Reader) error {
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("Failed to read sync response: %w", err)
		}
		if strings.HasPrefix(strings.TrimSpace(line), "+FULLRESYNC") {
			break
		}
	}

	var rdbHeader string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("Failed to read RDB header: %w", err)
		}
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "$") {
			rdbHeader = trimmed
			break
		}
	}
	rdbLen, err := strconv.Atoi(rdbHeader[1:])
	if err != nil {
		return fmt.Errorf("Invalid RDB length: %w", err)
	}

	_, err = io.CopyN(io.Discard, reader, int64(rdbLen))
	if err != nil {
		return fmt.Errorf("Failed to discard RDB payload: %w", err)
	}
	return nil
}

func ReadLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func ReadSimpleString(r *bufio.Reader, expected string) error {
	line, err := ReadLine(r)
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
