// Package remote lets actors on different processes or machines exchange
// messages over gRPC. It stays generic by wrapping payloads in
// google.protobuf.Any: any actor.Message that also implements proto.Message
// can be delivered to a remote actor without the transport needing to know
// its concrete type ahead of time.
package remote

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
	pb "github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor/remote/remotepb"
)

// tellTimeout bounds a single Tell call so a genuinely unreachable peer
// fails in bounded time rather than hanging forever. It deliberately does
// NOT use grpc.WaitForReady: Tell is used both for latency-sensitive FL
// traffic and for frequent best-effort cluster gossip, and the latter would
// otherwise pay a multi-second retry-backoff tax on every call to a peer
// that is briefly slow to answer. Callers that specifically need to wait out
// a peer's startup window (e.g. a Docker container not yet listening)
// should retry Tell themselves.
const tellTimeout = 10 * time.Second

// Server exposes a local ActorSystem to remote peers: every incoming
// Envelope is unwrapped and delivered to the actor named by TargetId.
type Server struct {
	pb.UnimplementedTransportServer
	System *actor.ActorSystem
}

// NewServer wraps system so it can be registered on a grpc.Server.
func NewServer(system *actor.ActorSystem) *Server {
	return &Server{System: system}
}

// Send implements the generated TransportServer interface.
func (s *Server) Send(ctx context.Context, env *pb.Envelope) (*pb.Ack, error) {
	msg, err := env.GetPayload().UnmarshalNew()
	if err != nil {
		return nil, fmt.Errorf("remote: unmarshal payload for %q: %w", env.GetTargetId(), err)
	}

	pid, ok := s.System.Lookup(env.GetTargetId())
	if !ok {
		return nil, fmt.Errorf("remote: unknown actor %q", env.GetTargetId())
	}
	if err := pid.Tell(msg); err != nil {
		return nil, fmt.Errorf("remote: deliver to %q: %w", env.GetTargetId(), err)
	}
	return &pb.Ack{}, nil
}

// Ref is an actor.Ref that forwards Tell calls to an actor named TargetID on
// a remote node, reachable through Client.
type Ref struct {
	TargetID string
	Client   pb.TransportClient
}

// ID returns the target actor's identifier.
func (r *Ref) ID() string { return r.TargetID }

// Tell wraps msg in a google.protobuf.Any and delivers it over gRPC. msg
// must implement proto.Message; plain Go structs used only for local
// messaging cannot be sent remotely.
func (r *Ref) Tell(msg actor.Message) error {
	pm, ok := msg.(proto.Message)
	if !ok {
		return fmt.Errorf("remote: message type %T does not implement proto.Message", msg)
	}
	payload, err := anypb.New(pm)
	if err != nil {
		return fmt.Errorf("remote: wrap payload: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), tellTimeout)
	defer cancel()
	_, err = r.Client.Send(ctx, &pb.Envelope{
		TargetId: r.TargetID,
		Payload:  payload,
	})
	return err
}

// Dial opens an insecure gRPC connection to a peer's Transport service at
// addr (e.g. "localhost:9001"). The caller is responsible for closing the
// returned connection.
func Dial(addr string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("remote: dial %s: %w", addr, err)
	}
	return conn, nil
}

// NewRef dials addr and returns a Ref addressing the actor targetID there,
// plus the underlying connection for the caller to Close when done with it.
func NewRef(addr, targetID string) (*Ref, *grpc.ClientConn, error) {
	conn, err := Dial(addr)
	if err != nil {
		return nil, nil, err
	}
	return &Ref{TargetID: targetID, Client: pb.NewTransportClient(conn)}, conn, nil
}
