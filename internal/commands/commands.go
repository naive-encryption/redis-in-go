// Package commands implements responses to resp parsed commands
package commands

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"redis-in-go/internal/resp"
	"redis-in-go/internal/store"
)

type Handler struct {
	conn     net.Conn
	builtIns map[string]func(args []string)
	store    *store.Store
}

func NewHandler(conn net.Conn) *Handler {
	store := store.NewStore()
	h := &Handler{conn: conn, store: store}
	h.builtIns = map[string]func(args []string){
		"echo":   h.echoCmd,
		"ping":   h.pingCmd,
		"set":    h.setCmd,
		"get":    h.getCmd,
		"rpush":  h.rpushCmd,
		"lrange": h.lrangeCmd,
		"lpush":  h.lpushCmd,
		"llen":   h.llenCmd,
		"lpop":   h.lpopCmd,
	}
	return h
}

func (h *Handler) lpopCmd(args []string) {
	poppedElements := ""
	if len(args) > 1 {
		val, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Println(err)
		}
		poppedElements = h.store.LPop(args[0], val)
	} else {
		poppedElements = h.store.LPop(args[0], 1)
	}
	fmt.Fprintf(h.conn, poppedElements)
}

func (h *Handler) llenCmd(args []string) {
	len := h.store.LLen(args[0])
	_, err := fmt.Fprintf(h.conn, ":%d\r\n", len)
	if err != nil {
		fmt.Println(err)
	}
}

func (h *Handler) lpushCmd(args []string) {
	length := h.store.LPush(args[0], args[1:]...)
	if length > 0 {
		fmt.Fprintf(h.conn, ":%d\r\n", length)
	}
}

func (h *Handler) lrangeCmd(args []string) {
	if len(args) < 3 {
		h.conn.Write([]byte("*0\r\n"))
	}

	start, err := strconv.Atoi(args[1])
	if err != nil {
		h.conn.Write([]byte("*0\r\n"))
	}

	stop, err := strconv.Atoi(args[2])
	if err != nil {
		h.conn.Write([]byte("*0\r\n"))
	}

	out := h.store.LRange(args[0], start, stop)

	if out == "" {
		h.conn.Write([]byte("*0\r\n"))
		return
	}

	fmt.Fprint(h.conn, out)
}

func (h *Handler) rpushCmd(args []string) {
	length := h.store.RPush(args[0], args[1:]...)
	if length > 0 {
		h.conn.Write([]byte(fmt.Sprintf(":%d\r\n", length)))
	}
}

func (h *Handler) pingCmd(args []string) {
	h.conn.Write([]byte("+PONG\r\n"))
}

func (h *Handler) echoCmd(args []string) {
	h.conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0])))
}

func (h *Handler) setCmd(args []string) {
	var ttl time.Duration
	if len(args) > 2 {
		switch strings.ToLower(args[2]) {
		case "ex":
			if len(args) > 3 {
				val, err := strconv.Atoi(args[3])
				if err != nil {
					h.conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
					return
				}
				ttl = time.Duration(val) * time.Second
			}
		case "px":
			if len(args) > 3 {
				val, err := strconv.Atoi(args[3])
				if err != nil {
					h.conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
					return
				}
				ttl = time.Duration(val) * time.Millisecond
			}
		}
	}
	h.store.Set(args[0], args[1], ttl) // TODO: check for nil
	h.conn.Write([]byte("+OK\r\n"))
}

func (h *Handler) getCmd(args []string) {
	val, err := h.store.Get(args[0])
	if err != nil {
		h.conn.Write([]byte("$-1\r\n"))
		return
	}
	h.conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)))
}

func (h *Handler) handleCommands(commands []string) {
	h.executeBuiltIn(strings.ToLower(commands[0]), commands[1:])
}

func (h *Handler) executeBuiltIn(cmd string, args []string) {
	builtInFunc, ok := h.builtIns[cmd]
	if !ok {
		fmt.Println("not a built-in")
	}

	builtInFunc(args)
}

func (h *Handler) HandleIncomingStream(conn net.Conn) {
	for {
		buf := make([]byte, 1024)

		_, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error with reading connection", err.Error())
			return
		}

		cmds, _, err := resp.ParseCommand(buf)
		if err != nil {
			fmt.Println(err)
			break
		}

		h.handleCommands(cmds)
	}
}
