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

func NewHandler(conn net.Conn, store *store.Store) *Handler {
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
		"blpop":  h.blpopCmd,
		"type":   h.typeCmd,
		"xadd":   h.xaddCmd,
		"xrange": h.xrangeCmd,
		"xread":  h.xreadCmd,
	}
	return h
}

func (h *Handler) xreadCmd(args []string) {
	if len(args) < 3 {
		return // TODO: specify error
	}

	streamKeys := make([]string, 0, (len(args)-1)/2)
	for i := 1; i <= (len(args)-1)/2; i++ {
		streamKeys = append(streamKeys, args[i])
	}
	streamEntryIDs := make([]string, 0, (len(args)-1)/2)
	for i := (len(args) - 1) / 2; i < len(args); i++ {
		streamEntryIDs = append(streamEntryIDs, args[i])
	}

	data := h.store.XRead(streamKeys, streamEntryIDs)
	response := fmt.Sprintf("*%d\r\n", len(data))
	for _, readEntry := range data {
		readEntryLenLine := "*2\r\n"
		readEntryIDLine := fmt.Sprintf("$%d\r\n%s\r\n", len(readEntry.StreamKey), readEntry.StreamKey)
		readEntryValuesLenLine := fmt.Sprintf("*%d\r\n", len(readEntry.Values))
		readEntryValuesLine := ""
		for _, readEntryValue := range readEntry.Values {
			mapLenStr := "*2\r\n"
			keyStr := fmt.Sprintf("$%d\r\n%s\r\n", len(readEntryValue.ID), readEntryValue.ID)
			valsStr := "*2\r\n"
			for _, valStr := range readEntryValue.Values {
				valsStr += fmt.Sprintf("$%d\r\n%s\r\n", len(valStr), valStr)
			}

			readEntryValuesLine = readEntryValuesLine + mapLenStr + keyStr + valsStr
		}
		response = response + readEntryLenLine + readEntryIDLine + readEntryValuesLenLine + readEntryValuesLine
	}
	fmt.Fprint(h.conn, response)
}

func (h *Handler) xrangeCmd(args []string) {
	if len(args) < 3 {
		return // TODO: specify error
	}
	data := h.store.XRange(args[0], args[1], args[2])
	response := fmt.Sprintf("*%d\r\n", len(data))
	for _, rangeEntry := range data {
		mapLenStr := "*2\r\n"
		keyStr := fmt.Sprintf("$%d\r\n%s\r\n", len(rangeEntry.ID), rangeEntry.ID)
		valsStr := "*2\r\n"
		for _, valStr := range rangeEntry.Values {
			valsStr += fmt.Sprintf("$%d\r\n%s\r\n", len(valStr), valStr)
		}

		response = response + mapLenStr + keyStr + valsStr
	}

	// fmt.Println(response)
	fmt.Fprint(h.conn, response)
}

func (h *Handler) xaddCmd(args []string) {
	values := make(map[string]string, len(args[2:]))
	for i := 2; i < len(args)-1; i += 2 {
		values[args[i]] = args[i+1]
	}
	response, err := h.store.XAdd(args[0], args[1], values)
	if err != nil {
		response = fmt.Sprintf("-%s\r\n", err.Error())
		fmt.Fprint(h.conn, response)
		return
	}
	response = fmt.Sprintf("$%d\r\n%s\r\n", len(response), response)
	fmt.Fprint(h.conn, response)
}

func (h *Handler) typeCmd(args []string) {
	if len(args) == 0 {
		return
	}
	response := fmt.Sprintf("+%s\r\n", h.store.Type(args[0]))
	fmt.Fprint(h.conn, response)
}

func (h *Handler) blpopCmd(args []string) {
	if len(args) < 2 {
		return
	}

	listKey := args[0]
	timeoutSeconds, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		fmt.Println(err)
	}
	timeout := time.Duration(timeoutSeconds * float64(time.Second))
	val, ok := h.store.BLPop(listKey, timeout)
	if !ok {
		fmt.Fprintf(h.conn, "*-1\r\n")
		return
	}
	response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(listKey), listKey, len(val), val)
	fmt.Fprint(h.conn, response)
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
