package actor

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTimeout = 2 * time.Second

// recorderActor appends every message it receives to a slice, guarded by a
// mutex, and signals a channel once per message so tests can wait for delivery.
type recorderActor struct {
	id       string
	mu       sync.Mutex
	received []Message
	notify   chan Message
}

func newRecorderActor(id string) *recorderActor {
	return &recorderActor{id: id, notify: make(chan Message, 16)}
}

func (a *recorderActor) ID() string { return a.id }

func (a *recorderActor) Receive(ctx *Context, msg Message) {
	a.mu.Lock()
	a.received = append(a.received, msg)
	a.mu.Unlock()
	a.notify <- msg
}

func (a *recorderActor) waitFor(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-a.notify:
		case <-time.After(testTimeout):
			t.Fatalf("timed out waiting for message %d", i+1)
		}
	}
}

func TestSpawnAndTell(t *testing.T) {
	system := NewActorSystem()
	defer system.Shutdown()

	a := newRecorderActor("recorder-1")
	pid, err := system.Spawn(a)
	require.NoError(t, err)

	require.NoError(t, pid.Tell("hello"))
	require.NoError(t, pid.Tell("world"))
	a.waitFor(t, 2)

	a.mu.Lock()
	defer a.mu.Unlock()
	assert.Equal(t, []Message{"hello", "world"}, a.received)
}

func TestDuplicateIDRejected(t *testing.T) {
	system := NewActorSystem()
	defer system.Shutdown()

	_, err := system.Spawn(newRecorderActor("dup"))
	require.NoError(t, err)

	_, err = system.Spawn(newRecorderActor("dup"))
	assert.Error(t, err)
}

func TestLookup(t *testing.T) {
	system := NewActorSystem()
	defer system.Shutdown()

	pid, err := system.Spawn(newRecorderActor("lookup-me"))
	require.NoError(t, err)

	found, ok := system.Lookup("lookup-me")
	require.True(t, ok)
	assert.Equal(t, pid, found)

	_, ok = system.Lookup("does-not-exist")
	assert.False(t, ok)
}

// toggleActor starts by recording messages under "loud" behavior; the first
// message makes it Become "quiet", which ignores everything after.
type toggleActor struct {
	id      string
	mu      sync.Mutex
	loudLog []Message
	notify  chan struct{}
}

func newToggleActor(id string) *toggleActor {
	return &toggleActor{id: id, notify: make(chan struct{}, 16)}
}

func (a *toggleActor) ID() string { return a.id }

func (a *toggleActor) Receive(ctx *Context, msg Message) {
	a.mu.Lock()
	a.loudLog = append(a.loudLog, msg)
	a.mu.Unlock()
	a.notify <- struct{}{}
	ctx.Become(a.quiet)
}

func (a *toggleActor) quiet(ctx *Context, msg Message) {
	a.notify <- struct{}{}
}

func TestBecomeSwitchesBehavior(t *testing.T) {
	system := NewActorSystem()
	defer system.Shutdown()

	a := newToggleActor("toggle-1")
	pid, err := system.Spawn(a)
	require.NoError(t, err)

	require.NoError(t, pid.Tell("first"))
	require.NoError(t, pid.Tell("second"))
	require.NoError(t, pid.Tell("third"))

	for i := 0; i < 3; i++ {
		select {
		case <-a.notify:
		case <-time.After(testTimeout):
			t.Fatalf("timed out waiting for message %d", i+1)
		}
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	assert.Equal(t, []Message{"first"}, a.loudLog)
}

// lifecycleActor records the order in which hooks and messages fire.
type lifecycleActor struct {
	id     string
	mu     sync.Mutex
	events []string
	done   chan struct{}
}

func newLifecycleActor(id string) *lifecycleActor {
	return &lifecycleActor{id: id, done: make(chan struct{})}
}

func (a *lifecycleActor) ID() string { return a.id }

func (a *lifecycleActor) OnStart(ctx *Context) {
	a.mu.Lock()
	a.events = append(a.events, "start")
	a.mu.Unlock()
}

func (a *lifecycleActor) Receive(ctx *Context, msg Message) {
	a.mu.Lock()
	a.events = append(a.events, "receive:"+msg.(string))
	a.mu.Unlock()
}

func (a *lifecycleActor) OnStop(ctx *Context) {
	a.mu.Lock()
	a.events = append(a.events, "stop")
	a.mu.Unlock()
	close(a.done)
}

func TestLifecycleHooksFireInOrder(t *testing.T) {
	system := NewActorSystem()

	a := newLifecycleActor("lifecycle-1")
	pid, err := system.Spawn(a)
	require.NoError(t, err)

	require.NoError(t, pid.Tell("ping"))
	system.Stop(pid)

	select {
	case <-a.done:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for OnStop")
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	assert.Equal(t, []string{"start", "receive:ping", "stop"}, a.events)
}

// panickyActor panics on "boom" and reports the panic through OnError,
// then keeps handling subsequent messages normally.
type panickyActor struct {
	id       string
	mu       sync.Mutex
	errs     []error
	received []Message
	notify   chan struct{}
}

func newPanickyActor(id string) *panickyActor {
	return &panickyActor{id: id, notify: make(chan struct{}, 16)}
}

func (a *panickyActor) ID() string { return a.id }

func (a *panickyActor) Receive(ctx *Context, msg Message) {
	defer func() { a.notify <- struct{}{} }()
	if msg == "boom" {
		panic("kaboom")
	}
	a.mu.Lock()
	a.received = append(a.received, msg)
	a.mu.Unlock()
}

func (a *panickyActor) OnError(ctx *Context, err error) {
	a.mu.Lock()
	a.errs = append(a.errs, err)
	a.mu.Unlock()
}

func TestPanicIsRecoveredAndActorSurvives(t *testing.T) {
	system := NewActorSystem()
	defer system.Shutdown()

	a := newPanickyActor("panicky-1")
	pid, err := system.Spawn(a)
	require.NoError(t, err)

	require.NoError(t, pid.Tell("boom"))
	require.NoError(t, pid.Tell("still-alive"))

	for i := 0; i < 2; i++ {
		select {
		case <-a.notify:
		case <-time.After(testTimeout):
			t.Fatalf("timed out waiting for delivery %d", i+1)
		}
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	require.Len(t, a.errs, 1)
	assert.Contains(t, a.errs[0].Error(), "panicky-1")
	assert.Equal(t, []Message{"still-alive"}, a.received)
}

func TestStopRejectsFurtherMessagesAndRemovesFromLookup(t *testing.T) {
	system := NewActorSystem()

	a := newRecorderActor("stoppable")
	pid, err := system.Spawn(a)
	require.NoError(t, err)

	system.Stop(pid)

	_, ok := system.Lookup("stoppable")
	assert.False(t, ok)

	err = pid.Tell("too-late")
	assert.ErrorIs(t, err, ErrMailboxClosed)
}

// pingPongActor replies to every message it gets from another actor,
// exercising Context.Send and Context.Spawn together.
type pingActor struct {
	id      string
	pongID  string
	replies chan Message
}

func (a *pingActor) ID() string { return a.id }

func (a *pingActor) Receive(ctx *Context, msg Message) {
	a.replies <- msg
}

type pongActor struct {
	id string
}

func (a *pongActor) ID() string { return a.id }

func (a *pongActor) Receive(ctx *Context, msg Message) {
	if req, ok := msg.(pingRequest); ok {
		_ = ctx.Send(req.replyTo, "pong")
	}
}

type pingRequest struct {
	replyTo *PID
}

func TestSendBetweenActors(t *testing.T) {
	system := NewActorSystem()
	defer system.Shutdown()

	ping := &pingActor{id: "ping", replies: make(chan Message, 1)}
	pingPID, err := system.Spawn(ping)
	require.NoError(t, err)

	pong := &pongActor{id: "pong"}
	pongPID, err := system.Spawn(pong)
	require.NoError(t, err)

	require.NoError(t, pongPID.Tell(pingRequest{replyTo: pingPID}))

	select {
	case msg := <-ping.replies:
		assert.Equal(t, "pong", msg)
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for pong reply")
	}
}
