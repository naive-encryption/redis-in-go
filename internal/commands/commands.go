// Package commands implements responses to resp parsed commands
package commands

import (
	"fmt"
	"net"
	"strings"

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
		"echo": h.echoCmd,
		"ping": h.pingCmd,
		"set":  h.setCmd,
		"get":  h.getCmd,
	}
	return h
}

func (h *Handler) pingCmd(args []string) {
	h.conn.Write([]byte("+PONG\r\n"))
}

func (h *Handler) echoCmd(args []string) {
	h.conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0])))
}

func (h *Handler) setCmd(args []string) {
	h.store.Set(args[0], args[1]) // TODO: check for nil
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
