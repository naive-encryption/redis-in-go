// Package store implements store types
package store

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type entry struct {
	value     string
	expiresAt time.Time
}

type Store struct {
	mu       sync.Mutex
	data     map[string]entry
	elements map[string][]string
}

func NewStore() *Store {
	return &Store{data: make(map[string]entry), elements: make(map[string][]string)}
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
	_, ok := s.elements[listKey]
	if !ok {
		s.elements[listKey] = make([]string, 0, len(value))
	}
	s.elements[listKey] = append(s.elements[listKey], value...)
	return len(s.elements[listKey])
}

func (s *Store) LRange(listKey string, start, stop int) string {
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
