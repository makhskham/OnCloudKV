// Package watch provides a pub/sub hub for key-change events,
// used to power the gRPC streaming Watch API.
package watch

import (
	"strings"
	"sync"
)

// EventType classifies a watch notification.
type EventType int

const (
	EventPut    EventType = 0
	EventDelete EventType = 1
)

// Event is a single key-change notification.
type Event struct {
	Type    EventType
	Key     string
	Value   []byte
	Version int64
}

type subscription struct {
	prefix string
	ch     chan Event
}

// Hub fans out FSM events to all matching prefix subscribers.
// It is the backbone of the Watch RPC.
type Hub struct {
	mu   sync.RWMutex
	subs map[uint64]*subscription
	next uint64
}

// NewHub creates a ready-to-use Hub.
func NewHub() *Hub {
	return &Hub{subs: make(map[uint64]*subscription)}
}

// Subscribe registers interest in events whose key starts with prefix.
// The returned channel receives events; call Unsubscribe(id) to deregister.
func (h *Hub) Subscribe(prefix string) (uint64, <-chan Event) {
	ch := make(chan Event, 64)
	h.mu.Lock()
	id := h.next
	h.next++
	h.subs[id] = &subscription{prefix: prefix, ch: ch}
	h.mu.Unlock()
	return id, ch
}

// Unsubscribe removes a subscription and closes its channel.
func (h *Hub) Unsubscribe(id uint64) {
	h.mu.Lock()
	if sub, ok := h.subs[id]; ok {
		close(sub.ch)
		delete(h.subs, id)
	}
	h.mu.Unlock()
}

// Notify dispatches e to every subscription whose prefix matches e.Key.
// It never blocks - full subscriber channels drop events rather than stall the FSM.
func (h *Hub) Notify(e Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, sub := range h.subs {
		if strings.HasPrefix(e.Key, sub.prefix) {
			select {
			case sub.ch <- e:
			default:
				// subscriber too slow - drop rather than block the FSM
			}
		}
	}
}
