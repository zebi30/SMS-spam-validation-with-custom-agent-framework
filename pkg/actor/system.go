package actor

import (
	"fmt"
	"sync"
)

// DefaultMailboxSize is the mailbox buffer capacity used by Spawn.
const DefaultMailboxSize = 128

// ActorSystem manages the lifecycle of actors: spawning, lookup and shutdown.
type ActorSystem struct {
	mu    sync.RWMutex
	cells map[string]*actorCell
}

// NewActorSystem creates an empty actor system ready to spawn actors.
func NewActorSystem() *ActorSystem {
	return &ActorSystem{
		cells: make(map[string]*actorCell),
	}
}

// Spawn starts a new actor with the default mailbox size.
func (s *ActorSystem) Spawn(a Actor) (*PID, error) {
	return s.SpawnWithMailboxSize(a, DefaultMailboxSize)
}

// SpawnWithMailboxSize starts a new actor with a custom mailbox buffer size.
func (s *ActorSystem) SpawnWithMailboxSize(a Actor, mailboxSize int) (*PID, error) {
	id := a.ID()

	s.mu.Lock()
	if _, exists := s.cells[id]; exists {
		s.mu.Unlock()
		return nil, fmt.Errorf("actor: id %q already registered", id)
	}
	cell := newActorCell(s, a, mailboxSize)
	s.cells[id] = cell
	s.mu.Unlock()

	cell.start()
	return cell.pid, nil
}

// Lookup returns the PID registered under id, if any.
func (s *ActorSystem) Lookup(id string) (*PID, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cell, ok := s.cells[id]
	if !ok {
		return nil, false
	}
	return cell.pid, true
}

// Stop terminates the actor referenced by pid, waits for it to exit, and
// removes it from the system. Any message sent to pid before Stop is called
// is still delivered first; only messages sent concurrently with (or after)
// the call may be rejected with ErrMailboxClosed instead.
func (s *ActorSystem) Stop(pid *PID) {
	s.mu.Lock()
	cell, ok := s.cells[pid.id]
	if ok {
		delete(s.cells, pid.id)
	}
	s.mu.Unlock()
	if !ok {
		return
	}
	cell.stop()
}

// Shutdown stops every actor currently registered in the system and waits
// for all of their goroutines to exit before returning.
func (s *ActorSystem) Shutdown() {
	s.mu.Lock()
	cells := make([]*actorCell, 0, len(s.cells))
	for _, cell := range s.cells {
		cells = append(cells, cell)
	}
	s.cells = make(map[string]*actorCell)
	s.mu.Unlock()

	for _, cell := range cells {
		cell.stop()
	}
}
