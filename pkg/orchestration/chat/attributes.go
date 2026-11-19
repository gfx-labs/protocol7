package chat

import (
	"context"

	"go.temporal.io/sdk/client"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"
)

// EnsureSearchAttributes registers the chat's search attributes in the
// namespace if they are missing (navi.EnsureSearchAttributes). Temporal
// refuses a start that sets an attribute the namespace does not have.
func EnsureSearchAttributes(ctx context.Context, temporalClient client.Client, namespace string) error {
	return navi.EnsureSearchAttributes(ctx, temporalClient, namespace)
}
