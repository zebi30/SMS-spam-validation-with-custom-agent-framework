package fl

import (
	"time"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
)

// sendWithRetry retries ctx.Send a few times with a short delay between
// attempts. FL round messages are sent once per round, so it is worth
// tolerating a peer that is still starting up (e.g. a Docker container
// whose gRPC server isn't listening yet) rather than failing the whole
// round on the very first attempt.
func sendWithRetry(ctx *actor.Context, to actor.Ref, msg actor.Message) error {
	const attempts = 10
	const delay = 500 * time.Millisecond

	var err error
	for i := 0; i < attempts; i++ {
		if err = ctx.Send(to, msg); err == nil {
			return nil
		}
		if i < attempts-1 {
			time.Sleep(delay)
		}
	}
	return err
}
