// Package store implements store types
package store

import (
	"errors"
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
