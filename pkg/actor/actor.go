// Package actor implements a generic actor framework: isolated units of
// state that communicate exclusively through asynchronous messages.
package actor

// Message is any payload exchanged between actors.
type Message any

// Actor is the unit of computation in the framework: independent state plus
// a single-threaded message handler reachable only through its mailbox.
type Actor interface {
	ID() string
	Receive(ctx *Context, msg Message)
}

// Behavior is a message handler an actor can switch to at runtime via
// Context.Become, replacing Receive for all messages from that point on.
type Behavior func(ctx *Context, msg Message)

// StartHook lets an actor run setup logic right before its mailbox loop starts.
type StartHook interface {
	OnStart(ctx *Context)
}

// StopHook lets an actor run cleanup logic after its mailbox loop has exited.
type StopHook interface {
	OnStop(ctx *Context)
}

// ErrorHook lets an actor recover from a panic raised while handling a
// message. Without it, the panic is contained and the actor keeps running.
type ErrorHook interface {
	OnError(ctx *Context, err error)
}
