package store

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"opspilot/internal/model"
)

// ErrNotFound is returned when an incident ID doesn't exist.
var ErrNotFound = errors.New("incident not found")

type MemoryStore struct {
	mu        sync.Mutex
	incidents map[int]model.Incident
	nextID    int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{incidents: make(map[int]model.Incident), nextID: 1}
}

func (s *MemoryStore) Create(_ context.Context, title, severity string) (model.Incident, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	inc := model.Incident{
		ID:        s.nextID,
		Title:     title,
		Severity:  severity,
		Status:    "open",
		CreatedAt: time.Now(),
	}
	s.incidents[inc.ID] = inc
	s.nextID++
	return inc, nil
}

func (s *MemoryStore) Get(_ context.Context, id int) (model.Incident, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	inc, ok := s.incidents[id]
	if !ok {
		return model.Incident{}, ErrNotFound
	}
	return inc, nil
}

func (s *MemoryStore) List(_ context.Context) ([]model.Incident, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list := make([]model.Incident, 0, len(s.incidents))
	for _, inc := range s.incidents {
		list = append(list, inc)
	}
	// map order is random, so sort by ID
	slices.SortFunc(list, func(a, b model.Incident) int { return a.ID - b.ID })
	return list, nil
}

func (s *MemoryStore) Update(_ context.Context, id int, title, severity, status string) (model.Incident, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	inc, ok := s.incidents[id]
	if !ok {
		return model.Incident{}, ErrNotFound
	}
	inc.Title = title
	inc.Severity = severity
	inc.Status = status
	s.incidents[id] = inc
	return inc, nil
}

func (s *MemoryStore) Delete(_ context.Context, id int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.incidents[id]; !ok {
		return ErrNotFound
	}
	delete(s.incidents, id)
	return nil
}
