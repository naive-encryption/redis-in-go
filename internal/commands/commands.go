// Package commands implements responses to resp parsed commands
package commands

import (
	"fmt"
	"net"
	"strings"

	"redis-in-go/internal/resp"
)

type Handler struct {
	conn     net.Conn
	builtIns map[string]func(args []string)
}

func NewHandler(conn net.Conn) *Handler {
	h := &Handler{conn: conn}
	h.builtIns = map[string]func(args []string){
		"echo": h.echoCmd,
		"ping": h.pingCmd,
	}
	return h
}

func (h *Handler) pingCmd(args []string) {
	h.conn.Write([]byte("+PONG\r\n"))
}

func (h *Handler) echoCmd(args []string) {
	h.conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0])))
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
