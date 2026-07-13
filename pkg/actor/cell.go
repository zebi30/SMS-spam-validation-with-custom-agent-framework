package actor

import "fmt"

// actorCell owns the goroutine, mailbox and current behavior of one running
// actor. It is never exposed directly; actors and callers only ever see it
// through a PID.
type actorCell struct {
	actor    Actor
	behavior Behavior
	mailbox  *mailbox
	pid      *PID
	system   *ActorSystem
	done     chan struct{}
}

func newActorCell(system *ActorSystem, a Actor, mailboxSize int) *actorCell {
	cell := &actorCell{
		actor:   a,
		mailbox: newMailbox(mailboxSize),
		done:    make(chan struct{}),
		system:  system,
	}
	cell.behavior = a.Receive
	cell.pid = &PID{id: a.ID(), cell: cell}
	return cell
}

// setBehavior swaps the handler used for subsequent messages. It is only
// ever called from within the actor's own run loop goroutine (via
// Context.Become), so it needs no synchronization.
func (c *actorCell) setBehavior(b Behavior) {
	c.behavior = b
}

func (c *actorCell) start() {
	ctx := &Context{self: c.pid, system: c.system, cell: c}
	if starter, ok := c.actor.(StartHook); ok {
		starter.OnStart(ctx)
	}
	go c.run(ctx)
}

// stopSignal is queued onto the same mailbox channel as regular messages so
// that stopping an actor never races with messages already sent to it: Go
// guarantees FIFO order for sends completed, in order, on one channel.
type stopSignal struct{}

func (c *actorCell) run(ctx *Context) {
	defer close(c.done)
	for msg := range c.mailbox.ch {
		if _, isStop := msg.(stopSignal); isStop {
			return
		}
		c.deliver(ctx, msg)
	}
}

func (c *actorCell) deliver(ctx *Context, msg Message) {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("actor %s: panic handling message: %v", c.pid.id, r)
			if handler, ok := c.actor.(ErrorHook); ok {
				handler.OnError(ctx, err)
			}
		}
	}()
	c.behavior(ctx, msg)
}

func (c *actorCell) stop() {
	c.mailbox.close()            // reject any further Tell calls from now on
	c.mailbox.ch <- stopSignal{} // delivered after every message already queued
	<-c.done
	if stopper, ok := c.actor.(StopHook); ok {
		ctx := &Context{self: c.pid, system: c.system, cell: c}
		stopper.OnStop(ctx)
	}
}
