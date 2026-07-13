package actor

import "errors"

// ErrMailboxClosed is returned when a message is sent to a stopped actor's mailbox.
var ErrMailboxClosed = errors.New("actor: mailbox closed")

// mailbox is a buffered channel of messages consumed by a single actor
// goroutine, giving asynchronous, non-blocking-until-full send semantics.
type mailbox struct {
	ch     chan Message
	closed chan struct{}
}

func newMailbox(size int) *mailbox {
	return &mailbox{
		ch:     make(chan Message, size),
		closed: make(chan struct{}),
	}
}

// enqueue delivers a message asynchronously, blocking only for buffer space.
func (m *mailbox) enqueue(msg Message) error {
	select {
	case <-m.closed:
		return ErrMailboxClosed
	default:
	}
	select {
	case m.ch <- msg:
		return nil
	case <-m.closed:
		return ErrMailboxClosed
	}
}

func (m *mailbox) close() {
	select {
	case <-m.closed:
	default:
		close(m.closed)
	}
}
