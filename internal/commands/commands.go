// Package commands implements responses to resp parsed commands
package commands

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"redis-in-go/internal/aof"
	"redis-in-go/internal/info"
	rdbparser "redis-in-go/internal/rdbParser"
	"redis-in-go/internal/resp"
	"redis-in-go/internal/store"
)

type CommandQueueEntry struct {
	cmd  string
	args []string
}

type ReplicaInfo struct {
	Conn   net.Conn
	Offset int64
}

type MasterNode struct {
	Replicas     map[string]*ReplicaInfo
	Mu           sync.RWMutex
	GlobalOffset int64
	AckChan      chan struct{}
}

type Handler struct {
	conn       net.Conn
	MasterNode *MasterNode
	builtIns   map[string]func(args []string)
	store      *store.Store

	cmdQueue          []CommandQueueEntry
	isMultiActive     bool
	isResponseQueued  bool
	cmdQueueResponses []string
	WatchedKeys       map[string]uint64

	isMasterConn bool

	aofBuffer []string
}

func InitHandler(conn net.Conn, store *store.Store, masterNode *MasterNode, isMasterConn bool) *Handler {
	h := NewHandler(conn, store, masterNode, isMasterConn)
	return h
}

func InitMasterNode() *MasterNode {
	masterNode := &MasterNode{
		Replicas: make(map[string]*ReplicaInfo),
		AckChan:  make(chan struct{}, 100),
	}
	return masterNode
}

func NewHandler(conn net.Conn, store *store.Store, masterNode *MasterNode, isMasterConn bool) *Handler {
	h := &Handler{conn: conn, store: store, MasterNode: masterNode, isMasterConn: isMasterConn}
	h.WatchedKeys = make(map[string]uint64)
	h.aofBuffer = make([]string, 0, 96)
	h.builtIns = map[string]func(args []string){
		"echo":     h.echoCmd,
		"ping":     h.pingCmd,
		"set":      h.setCmd, // propagated
		"get":      h.getCmd,
		"rpush":    h.rpushCmd, // propagated
		"lrange":   h.lrangeCmd,
		"lpush":    h.lpushCmd, // propagated
		"llen":     h.llenCmd,
		"lpop":     h.lpopCmd,  // propagated
		"blpop":    h.blpopCmd, // propagated
		"type":     h.typeCmd,
		"xadd":     h.xaddCmd, // propagated
		"xrange":   h.xrangeCmd,
		"xread":    h.xreadCmd,
		"incr":     h.incrCmd, // propagated
		"multi":    h.multiCmd,
		"exec":     h.execCmd,
		"discard":  h.discardCmd,
		"watch":    h.watchCmd,
		"unwatch":  h.unwatchCmd,
		"info":     h.infoCmd,
		"replconf": h.replconfCmd,
		"psync":    h.psyncCmd,
		"wait":     h.waitCmd,
		"config":   h.configCmds,
		"keys":     h.keysCmd,
		"save":     h.saveCmd,
	}
	return h
}

func (h *Handler) saveCmd(args []string) {
	// TODO: implement SAVE command
}

func (h *Handler) keysCmd(args []string) {
	path := info.WorkDir + "/" + info.RDBFileName

	parsedData, err := rdbparser.ReadRDBFile(path)
	if err != nil {
		fmt.Println("Failed to read rdb", err)
	}

	arrayHeader := fmt.Sprintf("*%d\r\n", len(parsedData))
	var sb strings.Builder
	for k := range parsedData {
		arrayElement := fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
		sb.Write([]byte(arrayElement))
	}
	response := arrayHeader + sb.String()
	h.SendResponse(response)
}

func (h *Handler) configCmds(args []string) {
	if len(args) < 2 {
		return
	}

	switch strings.ToLower(args[0]) {
	case "get":
		switch strings.ToLower(args[1]) {
		case "dir":
			response := fmt.Sprintf("*%d\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", 2, len(args[1]), args[1], len(info.WorkDir), info.WorkDir)
			h.SendResponse(response)
		case "dbfilename":
			response := fmt.Sprintf("*%d\r\n$%d\r\n$%d\r\n%s\r\n", 2, len(args[1]), args[1], len(info.RDBFileName), info.RDBFileName)
			h.SendResponse(response)
		case "appendonly", "appenddirname", "appendfilename", "appendfsync":
			value, err := aof.ConfigGet(strings.ToLower(args[1]))
			if err != nil {
				fmt.Println("Failed to retrieve aof info:", err)
			}
			response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(args[1]), args[1], len(value), value)
			fmt.Println(response)
			h.SendResponse(response)
		}
	}
}

func (m *MasterNode) CountSyncedReplicaUnlocked(targetOffset int64) int {
	count := 0
	for _, r := range m.Replicas {
		if r.Offset >= targetOffset {
			count++
		}
	}
	return count
}

func (m *MasterNode) BroadcastGetAck() {
	m.Mu.RLock()

	conns := make([]net.Conn, 0, len(m.Replicas))
	for _, r := range m.Replicas {
		conns = append(conns, r.Conn)
	}

	m.Mu.RUnlock()

	getAckCmd := "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
	payload := []byte(getAckCmd)

	for _, conn := range conns {
		if conn != nil {
			conn.Write(payload)
		}
	}
}

func (m *MasterNode) CountSyncedReplicas(targetOffset int64) int {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	return m.CountSyncedReplicaUnlocked(targetOffset)
}

func (h *Handler) waitCmd(args []string) {
	if len(args) < 2 {
		fmt.Println("Not enough arguments for wait command")
		return
	}
	numberOfReplicas, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Println("Failed to convert nubmer of replicas for wait command")
		return
	}
	timeoutMs, err := strconv.Atoi(args[1])
	if err != nil {
		fmt.Println("Failed to convert timeout for wait command")
		return
	}

	h.MasterNode.Mu.RLock()

	connectedRepilcasCount := len(h.MasterNode.Replicas)
	targetOffset := h.MasterNode.GlobalOffset

	if targetOffset == 0 || connectedRepilcasCount == 0 || numberOfReplicas == 0 {
		count := h.MasterNode.CountSyncedReplicaUnlocked(targetOffset)
		response := fmt.Sprintf(":%d\r\n", count)
		h.SendResponse(response)
		h.MasterNode.Mu.RUnlock()
		return
	}
	h.MasterNode.Mu.RUnlock()

	h.MasterNode.BroadcastGetAck()
	if h.MasterNode.CountSyncedReplicas(targetOffset) >= numberOfReplicas {
		response := fmt.Sprintf(":%d\r\n", h.MasterNode.CountSyncedReplicas(targetOffset))
		h.SendResponse(response)
		return
	}

	var timeoutChan <-chan time.Time
	if timeoutMs > 0 {
		timeoutChan = time.After(time.Duration(timeoutMs) * time.Millisecond)
	}

	for {
		if h.MasterNode.CountSyncedReplicas(targetOffset) >= numberOfReplicas {
			response := fmt.Sprintf(":%d\r\n", numberOfReplicas)
			h.SendResponse(response)
			return
		}

		select {
		case <-timeoutChan:
			response := fmt.Sprintf(":%d\r\n", h.MasterNode.CountSyncedReplicas(targetOffset))
			h.SendResponse(response)
			return
		case <-h.MasterNode.AckChan:
		}
	}
}

func (m *MasterNode) UpdateReplicaOffset(id string, offset int64) {
	m.Mu.Lock()
	if replica, exists := m.Replicas[id]; exists {
		replica.Offset = offset
	}
	m.Mu.Unlock()

	select {
	case m.AckChan <- struct{}{}:
	default:
	}
}

func (h *Handler) psyncCmd(args []string) {
	if len(args) < 2 {
		return // not enough arguments
	}

	addr := h.conn.RemoteAddr().String()

	h.MasterNode.Mu.Lock()
	if _, exists := h.MasterNode.Replicas[addr]; !exists {
		h.MasterNode.Replicas[addr] = &ReplicaInfo{}
	}
	h.MasterNode.Replicas[h.conn.RemoteAddr().String()].Conn = h.conn
	h.MasterNode.Mu.Unlock()

	response := fmt.Sprintf("+FULLRESYNC %s %d\r\n", info.MasterReplID, info.MasterReplOffset)
	h.SendResponse(response)
	emptyRDB := info.GetEmptyRDB()
	header := fmt.Sprintf("$%d\r\n", len(emptyRDB))
	h.SendResponse(header)
	_, err := h.conn.Write(emptyRDB)
	if err != nil {
		fmt.Println("Failed to send RDB file:", err)
	}
}

func (h *Handler) replconfCmd(args []string) {
	if len(args) < 1 {
		return // HACK:
	}
	subCommand := strings.ToLower(args[0])
	switch subCommand {
	case "listening-port":
		h.SendResponse("+OK\r\n")
	case "capa":
		h.SendResponse("+OK\r\n")
	case "getack":
		response := fmt.Sprintf("*3\r\n$8\r\nreplconf\r\n$3\r\nACK\r\n$%d\r\n%d\r\n", len(strconv.FormatInt(info.MasterReplOffset, 10)), info.MasterReplOffset)
		_, err := fmt.Fprint(h.conn, response)
		if err != nil {
			fmt.Println(err)
		}
	case "ack":

		if len(args) < 2 {
			return
		}
		ackOffset, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			fmt.Println("Failed to parse int on ack:", err)
			return
		}

		addr := h.conn.RemoteAddr().String()
		h.MasterNode.UpdateReplicaOffset(addr, ackOffset)
	}
}

func (h *Handler) infoCmd(args []string) {
	if len(args) == 0 {
		// full response
		return
	}

	response := ""

	switch args[0] {
	case "replication":
		roleField := fmt.Sprintf("role:%s\r\n", info.Role)
		masterReplIDField := fmt.Sprintf("master_replid:%s\r\n", info.MasterReplID)
		masterReplOffsetField := fmt.Sprintf("master_repl_offset:%d\r\n", info.MasterReplOffset)

		replicationResponse := roleField + masterReplIDField + masterReplOffsetField
		response = response + fmt.Sprintf("$%d\r\n%s\r\n", len(replicationResponse), replicationResponse)
		fmt.Println(response)
	default:
	}
	_, err := h.conn.Write([]byte(response))
	if err != nil {
		fmt.Println("Failed to respond to info command:", err)
	}
}

func (h *Handler) unwatchCmd(args []string) {
	h.discardWatch()
	response := "+OK\r\n"
	h.SendResponse(response)
}

func (h *Handler) watchCmd(args []string) {
	h.store.Watch(h.WatchedKeys, args)
	response := "+OK\r\n"
	h.SendResponse(response)
}

func (h *Handler) discardWatch() {
	h.isMultiActive = false
	h.isResponseQueued = false
	h.cmdQueueResponses = h.cmdQueueResponses[:0]
	h.cmdQueue = h.cmdQueue[:0]
	h.WatchedKeys = make(map[string]uint64)
}

func (h *Handler) discardCmd(args []string) {
	if !h.isMultiActive {
		response := "-ERR DISCARD without MULTI\r\n"
		h.SendResponse(response)
		return
	}
	h.discardWatch()
	response := "+OK\r\n"
	h.SendResponse(response)
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

	isChanged := h.store.CheckWatchedKeysForChange(h.WatchedKeys)
	if isChanged {
		h.discardWatch()
		response := "*-1\r\n"
		h.SendResponse(response)
		return
	}

	h.isResponseQueued = true
	h.isMultiActive = false
	for _, commandEntry := range h.cmdQueue {
		h.executeBuiltIn(commandEntry.cmd, commandEntry.args)
	}
	h.isResponseQueued = false

	arrayLenght := fmt.Sprintf("*%d\r\n", len(h.cmdQueueResponses))
	var sb strings.Builder
	sb.Write([]byte(arrayLenght))
	for _, cmdResponse := range h.cmdQueueResponses {
		sb.Write([]byte(cmdResponse))
	}
	h.cmdQueueResponses = h.cmdQueueResponses[:0]
	response := sb.String()
	h.SendResponse(response)
}

func (h *Handler) multiCmd(args []string) {
	response := "+OK\r\n"
	h.SendResponse(response)
	h.isMultiActive = true
}

func (h *Handler) incrCmd(args []string) {
	h.HandleModifyingCmd("incr", args)
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
	h.HandleModifyingCmd("xadd", args)
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
	h.HandleModifyingCmd("blpop", args)
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
	h.HandleModifyingCmd("lpop", args)
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
	h.HandleModifyingCmd("lpush", args)
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
	h.HandleModifyingCmd("rpush", args)
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
	h.HandleModifyingCmd("set", args) // TODO: check for len before

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
	if h.isMultiActive && cmd == "watch" {
		response := "-ERR WATCH inside MULTI is not allowed\r\n"
		h.SendResponse(response)
		return
	}
	if h.isMultiActive && cmd != "exec" && cmd != "discard" {
		newEntry := CommandQueueEntry{cmd: cmd, args: args}
		h.cmdQueue = append(h.cmdQueue, newEntry)
		response := "+QUEUED\r\n"
		h.SendResponse(response)
		return
	}

	builtInFunc(args)
}

func (h *Handler) HandleIncomingStream(conn net.Conn, reader *bufio.Reader) {
	var buf []byte
	for {
		if len(buf) == 0 {
			b := make([]byte, 1024)
			n, err := reader.Read(b)
			if err != nil {
				fmt.Println("Error with reading connection:", err)
				return
			}
			buf = b[:n]
		}

		cmds, pos, err := resp.ParseCommand(buf)
		if err != nil {
			if err.Error() == "not enough bytes for bulk string" || err.Error() == "invalid input, expected array" {
				b := make([]byte, 1024)
				n, readErr := reader.Read(b)
				if readErr != nil {
					fmt.Println("Error reading more data:", readErr)
					return
				}
				buf = append(buf, b[:n]...)
				continue
			}
			fmt.Println("Parse error:", err)
			break
		}
		h.handleCommands(cmds)
		h.TrackOffset(int64(pos))
		buf = buf[pos:]
	}
}

func (m *MasterNode) AddGlobalOffset(offsetToAdd int64) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.GlobalOffset += offsetToAdd
}

func (h *Handler) TrackOffset(offsetToAdd int64) {
	if info.Role == "slave" {
		info.MasterReplOffset += offsetToAdd
	}
}

func (h *Handler) SendResponse(response string) {
	if h.isMasterConn {
		return
	}
	if h.isResponseQueued {
		h.cmdQueueResponses = append(h.cmdQueueResponses, response)
	} else {
		_, err := fmt.Fprint(h.conn, response)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func (m *MasterNode) PropagateToReplicas(cmd string, args []string) {
	m.Mu.RLock()
	defer m.Mu.RUnlock()

	formated := formatPropagatedCommand(cmd, args)
	for addr, info := range m.Replicas {
		_, err := info.Conn.Write([]byte(formated))
		if err != nil {
			fmt.Printf("Failed to propaget to replicas %s: %v\n", addr, err)
		}
	}
	m.GlobalOffset += int64(len(formated))
}

func formatPropagatedCommand(cmd string, args []string) string {
	var sb strings.Builder

	sb.Write([]byte(fmt.Sprintf("*%d\r\n", len(args)+1)))
	sb.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(cmd), cmd)))
	for _, arg := range args {
		sb.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)))
	}

	return sb.String()
}

func (h *Handler) FlushWriteCmdsToAOF() {
	fileNameToFlushTo := aof.GetFileNameToFlushAOFTo()
	aofDir, exists := aof.AOFInfo["appenddirname"]
	if !exists {
		fmt.Println("No directory for aof set")
		return
	}
	fileNameToFlushToFullPath := filepath.Join(info.WorkDir, aofDir, fileNameToFlushTo)

	file, err := os.OpenFile(fileNameToFlushToFullPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Println("Failed to open active incr file:", err)
		return
	}
	defer file.Close()

	if len(h.aofBuffer) > 0 {
		for _, cmd := range h.aofBuffer {
			file.WriteString(cmd)
		}
		h.aofBuffer = h.aofBuffer[:0]

	}
}

func (h *Handler) HandleModifyingCmd(cmd string, args []string) {
	if h.MasterNode != nil {
		h.MasterNode.PropagateToReplicas(cmd, args)
	}

	arrayHeader := fmt.Sprintf("*%d\r\n", len(args)+1)
	var sb strings.Builder
	sb.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(cmd), strings.ToUpper(cmd)))) // HACK: ToUpper doesn't neccessarily reflects the actual cmd passed
	for _, arg := range args {
		sb.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)))
	}
	aofLine := arrayHeader + sb.String()

	h.aofBuffer = append(h.aofBuffer, aofLine)

	h.FlushWriteCmdsToAOF()
}
