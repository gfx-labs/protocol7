package review

import (
	"context"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"

	"go.temporal.io/sdk/client"
)

// File files the review ticket of a pull request. A pull request that already
// has a running review gets a rereview signal instead of a second review, so
// filing it again after a push is how an author asks for another look.
func File(ctx context.Context, temporal client.Client, request Request) (workflowID string, err error) {
	return navi.FileReview(ctx, temporal, request)
}
