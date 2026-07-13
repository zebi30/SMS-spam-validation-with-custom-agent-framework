package actor

// PID (process identifier) is a lightweight, safe-to-copy reference used to
// send messages to an actor without exposing its internal state.
type PID struct {
	id   string
	cell *actorCell
}

// ID returns the unique identifier of the referenced actor.
func (p *PID) ID() string {
	return p.id
}

// Tell asynchronously delivers msg to the actor's mailbox.
func (p *PID) Tell(msg Message) error {
	return p.cell.mailbox.enqueue(msg)
}
