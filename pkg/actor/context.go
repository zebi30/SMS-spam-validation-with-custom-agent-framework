package actor

// Context gives an actor access to itself, the system, and behavior control
// while it handles a single message.
type Context struct {
	self   *PID
	system *ActorSystem
	cell   *actorCell
}

// Self returns the PID of the actor currently handling a message.
func (c *Context) Self() *PID {
	return c.self
}

// System returns the ActorSystem the actor belongs to.
func (c *Context) System() *ActorSystem {
	return c.system
}

// Send delivers a message to another actor's mailbox, local or remote.
func (c *Context) Send(to Ref, msg Message) error {
	return to.Tell(msg)
}

// Become replaces the actor's message handler starting with the next
// message; the current message continues to run under the old behavior.
func (c *Context) Become(behavior Behavior) {
	c.cell.setBehavior(behavior)
}

// Spawn creates a new actor within the same system as the caller.
func (c *Context) Spawn(a Actor) (*PID, error) {
	return c.system.Spawn(a)
}

// Stop terminates the given actor.
func (c *Context) Stop(pid *PID) {
	c.system.Stop(pid)
}
