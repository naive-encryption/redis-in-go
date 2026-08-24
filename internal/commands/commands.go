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

type CommandQueueEntry struct {
	cmd  string
	args []string
}

type Handler struct {
	conn     net.Conn
	builtIns map[string]func(args []string)
	store    *store.Store

	cmdQueue          []CommandQueueEntry
	isMultiActive     bool
	isResponseQueued  bool
	cmdQueueResponses []string
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
		"incr":   h.incrCmd,
		"multi":  h.multiCmd,
		"exec":   h.execCmd,
	}
	return h
}

func (h *Handler) execCmd(args []string) {
	if !h.isMultiActive {
		response := "-ERR EXEC without MULTI\r\n"
		h.SendResponse(response)
		return
	}

	if len(h.cmdQueue) == 0 {
		response := "*0\r\n"
		h.SendResponse(response)
		h.isMultiActive = false
		return
	}

	h.isResponseQueued = true
	h.isMultiActive = false
	for _, commandEntry := range h.cmdQueue {
		h.executeBuiltIn(commandEntry.cmd, commandEntry.args)
	}
	h.cmdQueueResponses = nil
	h.isResponseQueued = false

	arrayLenght := fmt.Sprintf("*%d\r\n", len(h.cmdQueueResponses))
	var sb strings.Builder
	sb.Write([]byte(arrayLenght))
	for _, cmdResponse := range h.cmdQueueResponses {
		sb.Write([]byte(cmdResponse))
	}
	response := sb.String()
	h.SendResponse(response)
}

func (h *Handler) multiCmd(args []string) {
	response := "+OK\r\n"
	h.SendResponse(response)
	h.isMultiActive = true
}

func (h *Handler) incrCmd(args []string) {
	if len(args) == 0 {
		return // TODO: specify error
	}
	data, err := h.store.INCR(args[0])
	if err != nil {
		response := "-ERR value is not an integer or out of range\r\n"
		h.SendResponse(response)
		return
	}

	response := fmt.Sprintf(":%d\r\n", data)
	h.SendResponse(response)
}

func (h *Handler) xreadCmd(args []string) {
	if len(args) < 3 {
		return // TODO: specify error
	}

	var blockForMs int
	if args[0] == "block" {
		ms, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Println(err) // TODO: make error a response
		}
		blockForMs = ms
	}

	streamWordIndex := 0
	for index, word := range args {
		if word == "streams" {
			streamWordIndex = index
		}
	}

	keysAndIDs := args[streamWordIndex:]
	numStreams := (len(keysAndIDs) - 1) / 2
	streamKeys := make([]string, numStreams)
	streamEntryIDs := make([]string, numStreams)

	for i := 0; i < numStreams; i++ {
		streamKeys[i] = keysAndIDs[i+1]
		streamEntryIDs[i] = keysAndIDs[1+numStreams+i]
	}

	data := h.store.XRead(streamKeys, streamEntryIDs, blockForMs)
	if data == nil {
		response := "*-1\r\n"
		h.SendResponse(response)
		return
	}
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
	h.SendResponse(response)
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

	h.SendResponse(response)
}

func (h *Handler) xaddCmd(args []string) {
	values := make(map[string]string, len(args[2:]))
	for i := 2; i < len(args)-1; i += 2 {
		values[args[i]] = args[i+1]
	}
	response, err := h.store.XAdd(args[0], args[1], values)
	if err != nil {
		response = fmt.Sprintf("-%s\r\n", err.Error())
		h.SendResponse(response)
		return
	}
	response = fmt.Sprintf("$%d\r\n%s\r\n", len(response), response)
	h.SendResponse(response)
}

func (h *Handler) typeCmd(args []string) {
	if len(args) == 0 {
		return
	}
	response := fmt.Sprintf("+%s\r\n", h.store.Type(args[0]))
	h.SendResponse(response)
}

func (h *Handler) blpopCmd(args []string) {
	if len(args) < 2 {
		return
	}

	listKey := args[0]
	timeoutSeconds, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		fmt.Println(err) // TODO: make error a response
	}
	timeout := time.Duration(timeoutSeconds * float64(time.Second))
	val, ok := h.store.BLPop(listKey, timeout)
	if !ok {
		response := "*-1\r\n"
		h.SendResponse(response)
		return
	}
	response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(listKey), listKey, len(val), val)
	h.SendResponse(response)
}

func (h *Handler) lpopCmd(args []string) {
	poppedElements := ""
	if len(args) > 1 {
		val, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Println(err) // TODO: make error a response
		}
		poppedElements = h.store.LPop(args[0], val)
	} else {
		poppedElements = h.store.LPop(args[0], 1)
	}
	h.SendResponse(poppedElements)
}

func (h *Handler) llenCmd(args []string) {
	len := h.store.LLen(args[0])
	response := fmt.Sprintf(":%d\r\n", len)
	h.SendResponse(response)
}

func (h *Handler) lpushCmd(args []string) {
	length := h.store.LPush(args[0], args[1:]...)
	if length > 0 {
		response := fmt.Sprintf(":%d\r\n", length)
		h.SendResponse(response)
	}
}

func (h *Handler) lrangeCmd(args []string) {
	if len(args) < 3 {
		response := "*0\r\n"
		h.SendResponse(response)
	}

	start, err := strconv.Atoi(args[1])
	if err != nil {
		response := "*0\r\n"
		h.SendResponse(response)
	}

	stop, err := strconv.Atoi(args[2])
	if err != nil {
		response := "*0\r\n"
		h.SendResponse(response)
	}

	out := h.store.LRange(args[0], start, stop)

	if out == "" {
		response := "*0\r\n"
		h.SendResponse(response)
		return
	}

	h.SendResponse(out)
}

func (h *Handler) rpushCmd(args []string) {
	length := h.store.RPush(args[0], args[1:]...)
	if length > 0 {
		response := fmt.Sprintf(":%d\r\n", length)
		h.SendResponse(response)
	}
}

func (h *Handler) pingCmd(args []string) {
	response := "+PONG\r\n"
	h.SendResponse(response)
}

func (h *Handler) echoCmd(args []string) {
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0])
	h.SendResponse(response)
}

func (h *Handler) setCmd(args []string) {
	var ttl time.Duration
	if len(args) > 2 {
		switch strings.ToLower(args[2]) {
		case "ex":
			if len(args) > 3 {
				val, err := strconv.Atoi(args[3])
				if err != nil {
					response := "-ERR value is not an integer or out of range\r\n"
					h.SendResponse(response)
					return
				}
				ttl = time.Duration(val) * time.Second
			}
		case "px":
			if len(args) > 3 {
				val, err := strconv.Atoi(args[3])
				if err != nil {
					response := "-ERR value is not an integer or out of range\r\n"
					h.SendResponse(response)
					return
				}
				ttl = time.Duration(val) * time.Millisecond
			}
		}
	}
	h.store.Set(args[0], args[1], ttl) // TODO: check for nil
	response := "+OK\r\n"
	h.SendResponse(response)
}

func (h *Handler) getCmd(args []string) {
	val, err := h.store.Get(args[0])
	if err != nil {
		response := "$-1\r\n"
		h.SendResponse(response)
		return
	}
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
	h.SendResponse(response)
}

func (h *Handler) handleCommands(commands []string) {
	h.executeBuiltIn(strings.ToLower(commands[0]), commands[1:])
}

func (h *Handler) executeBuiltIn(cmd string, args []string) {
	builtInFunc, ok := h.builtIns[cmd]
	if !ok {
		fmt.Println("not a built-in")
	}
	if h.isMultiActive && cmd != "exec" {
		newEntry := CommandQueueEntry{cmd: cmd, args: args}
		h.cmdQueue = append(h.cmdQueue, newEntry)
		response := "+QUEUED\r\n"
		h.SendResponse(response)
		return
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

func (h *Handler) SendResponse(response string) {
	if h.isResponseQueued {
		h.cmdQueueResponses = append(h.cmdQueueResponses, response)
	} else {
		_, err := fmt.Fprint(h.conn, response)
		if err != nil {
			fmt.Println(err)
		}
	}
}
