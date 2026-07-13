package actor

// Ref is anything a message can be Tell'd to: a local actor's PID, or a
// reference that forwards over the network to an actor on another node.
// Application code sends through Context.Send without caring which kind of
// Ref it holds.
type Ref interface {
	ID() string
	Tell(msg Message) error
}
