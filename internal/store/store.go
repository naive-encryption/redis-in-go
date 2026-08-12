// Package store implements store types
package store

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type streamEntry struct {
	ID     string
	Values map[string]string
}

type Stream struct {
	Entries []streamEntry
}

type entry struct {
	value     string
	expiresAt time.Time
}

type Store struct {
	mu              sync.Mutex
	data            map[string]entry
	elements        map[string][]string
	blockingClients map[string][]chan string
	streams         map[string]Stream
}

func NewStore() *Store {
	return &Store{
		data:            make(map[string]entry),
		elements:        make(map[string][]string),
		blockingClients: make(map[string][]chan string),
		streams:         make(map[string]Stream),
	}
}

func (s *Store) Set(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := entry{value: value}
	if ttl > 0 {
		e.expiresAt = time.Now().Add(ttl)
	}
	s.data[key] = e
}

func (s *Store) Get(key string) (value string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.data[key]
	if !ok {
		return "", errors.New("key doesn't exist")
	}
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		delete(s.data, key)
		return "", errors.New("key expired")
	}
	return entry.value, nil
}

func (s *Store) RPush(listKey string, value ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if length, handled := s.checkForBlockingClients(listKey, false, value...); handled {
		return length
	}
	_, ok := s.elements[listKey]
	if !ok {
		s.elements[listKey] = make([]string, 0, len(value))
	}
	s.elements[listKey] = append(s.elements[listKey], value...)
	return len(s.elements[listKey])
}

func (s *Store) LRange(listKey string, start, stop int) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.elements[listKey]
	if !ok {
		return ""
	}

	if start < 0 {
		start = len(s.elements[listKey]) + start
		start = max(start, 0)
	}
	if stop < 0 {
		stop = len(s.elements[listKey]) + stop
		stop = max(stop, 0)
	}

	if start >= len(s.elements[listKey]) || start > stop {
		return ""
	}
	if stop >= len(s.elements[listKey]) {
		stop = len(s.elements[listKey]) - 1
	}

	out := make([]string, 0, 100) // HACK:
	out = append(out, fmt.Sprintf("*%d\r\n", stop-start+1))

	for i := start; i <= stop; i++ {
		out = append(out, fmt.Sprintf("$%d\r\n", len(s.elements[listKey][i])))
		out = append(out, fmt.Sprintf("%s\r\n", s.elements[listKey][i]))
	}
	return strings.Join(out, "")
}

func (s *Store) LPush(listKey string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	reversed := make([]string, len(values))
	for i, v := range values {
		reversed[len(values)-1-i] = v
	}

	if length, handled := s.checkForBlockingClients(listKey, true, reversed...); handled {
		return length
	}

	s.elements[listKey] = append(reversed, s.elements[listKey]...)
	return len(s.elements[listKey])
}

func (s *Store) LLen(listKey string) int {
	_, ok := s.elements[listKey]
	if !ok {
		return 0
	}
	return len(s.elements[listKey])
}

func (s *Store) LPop(listKey string, numberElements int) string {
	_, ok := s.elements[listKey]
	if !ok {
		return "$-1\r\n"
	}
	if numberElements >= len(s.elements[listKey]) {
		numberElements = len(s.elements[listKey])
	}

	out := make([]string, 0, 100) // HACK:
	if numberElements > 1 {
		out = append(out, fmt.Sprintf("*%d\r\n", numberElements))
	}
	for i := 0; i < numberElements; i++ {
		elem := s.elements[listKey][i]
		out = append(out, fmt.Sprintf("$%d\r\n", len(elem)))
		out = append(out, fmt.Sprintf("%s\r\n", elem))
	}
	s.elements[listKey] = s.elements[listKey][numberElements:]
	return strings.Join(out, "")
}

func (s *Store) BLPop(listKey string, timeout time.Duration) (string, bool) {
	s.mu.Lock()

	if list, exists := s.elements[listKey]; exists && len(list) > 0 {
		val := list[0]
		s.elements[listKey] = list[1:]
		s.mu.Unlock()
		return val, true
	}

	ch := make(chan string, 1)
	s.blockingClients[listKey] = append(s.blockingClients[listKey], ch)

	s.mu.Unlock()

	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutChan = time.After(timeout)
	}

	select {
	case val := <-ch:
		return val, true
	case <-timeoutChan:
		s.mu.Lock()
		s.removeWaiter(listKey, ch)
		s.mu.Unlock()

		select {
		case val := <-ch:
			return val, true
		default:
			return "", false
		}
	}
}

func (s *Store) removeWaiter(listKey string, targetChain chan string) {
	waiters := s.blockingClients[listKey]

	for i, ch := range waiters {
		if ch == targetChain {
			s.blockingClients[listKey] = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}

	if len(s.blockingClients[listKey]) == 0 {
		delete(s.blockingClients, listKey)
	}
}

// return int if elements are added
func (s *Store) checkForBlockingClients(listKey string, isLeftPush bool, value ...string) (int, bool) {
	waiters, exists := s.blockingClients[listKey]
	if !exists || len(waiters) == 0 {
		return 0, false
	}

	ch := waiters[0]
	s.blockingClients[listKey] = waiters[1:]
	if len(s.blockingClients[listKey]) == 0 {
		delete(s.blockingClients, listKey)
	}

	ch <- value[0]
	remainingVals := value[1:]

	if len(remainingVals) > 0 {
		if _, ok := s.elements[listKey]; !ok {
			s.elements[listKey] = make([]string, 0, len(remainingVals))
		}
		if isLeftPush {
			s.elements[listKey] = append(remainingVals, s.elements[listKey]...)
		} else {
			s.elements[listKey] = append(s.elements[listKey], remainingVals...)
		}
	}
	return len(s.elements[listKey]) + 1, true
}

func (s *Store) Type(listKey string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[listKey]; ok {
		return "string"
	}
	if _, ok := s.streams[listKey]; ok {
		return "stream"
	}
	return "none"
}

func (s *Store) XAdd(streamKey string, id string, values map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.streams[streamKey]

	var lastID string

	if ok && len(stream.Entries) > 0 {
		lastID = stream.Entries[len(stream.Entries)-1].ID
	}

	finalID, err := resolveStreamID(id, lastID)
	if err != nil {
		return "", err
	}

	entry := streamEntry{ID: finalID, Values: values}

	if !ok {
		s.streams[streamKey] = Stream{Entries: []streamEntry{entry}}
	} else {
		stream.Entries = append(stream.Entries, entry)
		s.streams[streamKey] = stream
	}
	return finalID, nil
}

func resolveStreamID(requestedID, lastID string) (string, error) {
	var lastMs, lastSq int

	if lastID != "" {
		lastMs, lastSq, _ = convertID(lastID)
	}

	if requestedID == "*" {
		nowMs := int(time.Now().UnixMilli())

		nowMs = max(nowMs, lastMs)

		newSq := 0
		if nowMs == lastMs {
			newSq = lastSq + 1
		}
		return fmt.Sprintf("%d-%d", nowMs, newSq), nil
	}

	newMs, newSq, err := convertID(requestedID)
	if err != nil {
		return "", errors.New("ERR Invalid stream ID specified as argument")
	}

	if newSq == -1 {
		if lastID != "" && newMs == lastMs {
			newSq = lastSq + 1
		} else if newMs == 0 {
			newSq = 1
		} else {
			newSq = 0
		}
	}

	if newMs == 0 && newSq == 0 {
		return "", errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}
	if lastID != "" && (newMs < lastMs || (newMs == lastMs && newSq <= lastSq)) {
		return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	return fmt.Sprintf("%d-%d", newMs, newSq), nil
}

func convertID(id string) (int, int, error) {
	splitID := strings.Split(id, "-")
	if len(splitID) != 2 {
		return 0, 0, errors.New("invalid ID format")
	}
	milisecondsTime, err := strconv.Atoi(splitID[0])
	if err != nil {
		return 0, 0, err
	}

	var sequenceNumber int
	if splitID[1] == "*" {
		sequenceNumber = -1
	} else {
		sequenceNumber, err = strconv.Atoi(splitID[1])
		if err != nil {
			return 0, 0, err
		}

	}
	return milisecondsTime, sequenceNumber, nil
}

type RangeEntry struct {
	ID     string
	Values []string
}

func (s *Store) XRange(streamKey string, startID, endID string) []RangeEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.streams[streamKey]
	if !ok {
		return nil // TODO: specify the error
	}

	if startID == "-" {
		startID = "0-1"
	}
	if endID == "+" {
		endID = s.streams[streamKey].Entries[len(s.streams[streamKey].Entries)-1].ID
	}
	startStreamEntryIndex, endStreamEntryIndex := s.convertIDToIndexInEntry(streamKey, startID, endID)

	out := make([]RangeEntry, 0, endStreamEntryIndex-startStreamEntryIndex)

	for i := startStreamEntryIndex; i < endStreamEntryIndex; i++ {
		entry := stream.Entries[i]
		entryValsLen := len(entry.Values)

		flatValues := make([]string, 0, entryValsLen*2) //*2 since both key and value are needed
		for k, v := range entry.Values {
			flatValues = append(flatValues, k, v)
		}
		out = append(out, RangeEntry{ID: entry.ID, Values: flatValues})
	}
	return out
}

func (s *Store) convertIDToIndexInEntry(streamKey, start, end string) (int, int) {
	stream, exists := s.streams[streamKey]
	if !exists {
		return 0, 0
	}
	entries := stream.Entries

	startID, _ := slices.BinarySearchFunc(entries, start, func(entry streamEntry, target string) int {
		return compareStreamIDs(entry.ID, target)
	})
	endID, found := slices.BinarySearchFunc(entries, end, func(entry streamEntry, target string) int {
		return compareStreamIDs(entry.ID, target)
	})

	if found {
		endID++ // XRange is inclusive
	}
	return startID, endID
}

func compareStreamIDs(id1, id2 string) int {
	ms1, sq1, _ := convertID(id1)
	ms2, sq2, _ := convertID(id2)

	if ms1 != ms2 {
		return cmp.Compare(ms1, ms2)
	}
	return cmp.Compare(sq1, sq2)
}
