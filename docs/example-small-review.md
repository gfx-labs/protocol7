# Small example: internal PR review

This is a small jobcity-style review stage, not the complete pipeline. The complete one is worked through in `example-pr-flows.md` and the three workflow files it names, where a review workflow follows a pull request across heads and a separate workflow merges. This file checks out an exact PR head, asks a read-only agent to run repository checks and review the change, and returns a typed report. It does not publish a GitHub review or merge. Those are consumer-owned activities with current-head checks and operation-specific idempotency.

The p7 SDK is proposed, not implemented. The names below follow the current design and have not been compiled against an SDK. The consumer owns the protobuf types, prompts, worker registration and PDP assignments. The configured session uses a pinned image/model and repository-scoped read-only PDP grants. The prompt is not the security boundary.

```proto
syntax = "proto3";
package review.v1;

message ReviewRequest {
  string repository = 1;
  string base = 2;
  string head = 3;
}

message ReviewReport {
  enum Outcome {
    OUTCOME_UNSPECIFIED = 0;
    APPROVE = 1;
    CHANGES_REQUESTED = 2;
    BLOCKED = 3;
  }
  Outcome outcome = 1;
  string summary = 2;
  repeated string evidence = 3;
}
```

```go
package reviews

import (
    "errors"
    "fmt"
    "slices"
    "time"

    "github.com/gfx-labs/protocol7/pkg/p7"
    reviewv1 "example.com/selfdev/gen/review/v1"
    "go.temporal.io/sdk/workflow"
)

type Config struct {
    Repository    string
    ReviewSession p7.SessionSpec
}

type Workflows struct {
    config Config
}

func Register(worker *p7.Worker, config Config) {
    workflows := &Workflows{config: config}
    p7.Register(worker, "pr_review", workflows.Review)
}

func (w *Workflows) Review(
    ctx workflow.Context,
    ticket p7.Ticket[*reviewv1.ReviewRequest],
) (report *reviewv1.ReviewReport, err error) {
    request := ticket.Fields
    if request == nil || request.Repository != w.config.Repository ||
        request.Base == "" || request.Head == "" {
        return nil, fmt.Errorf("review requires the configured repository and exact base/head revisions")
    }

    spec := w.config.ReviewSession
    spec.Inputs = append(slices.Clone(spec.Inputs), p7.Input{
        Source:      p7.GitClone{Repository: request.Repository, Ref: request.Head},
        Destination: "/work",
    })
    session, err := p7.OpenSession(ctx, spec)
    if err != nil {
        return nil, fmt.Errorf("open reviewer: %w", err)
    }
    defer func() {
        cleanupCtx, cancel := workflow.NewDisconnectedContext(ctx)
        defer cancel()
        cleanupCtx = workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
            StartToCloseTimeout: time.Minute,
            ScheduleToCloseTimeout: 2 * time.Minute,
        })
        err = errors.Join(err, session.Close(cleanupCtx))
    }()

    prompt := fmt.Sprintf(
        "Review repository %s, base %s, head %s. Inspect the diff, run the repository's documented checks, and return ReviewReport with findings and check evidence. Do not edit, publish, or merge.",
        request.Repository, request.Base, request.Head,
    )
    report, err = p7.Prompt[*reviewv1.ReviewReport](ctx, session, prompt)
    if err != nil {
        return nil, fmt.Errorf("review PR head: %w", err)
    }
    if report == nil || report.Outcome == reviewv1.ReviewReport_OUTCOME_UNSPECIFIED {
        return nil, fmt.Errorf("reviewer returned no decision")
    }
    return report, nil
}
```

Consumer intake files the `pr_review` ticket with exact revisions and a business deduplication key scoped to repository/head/review cycle. That is one ticket per reviewed head. The complete example files one ticket per pull request and records each review as a round on it. Temporal controls execution and retries. p7 persists the typed outcome and run evidence. Before any later publication, a consumer activity rechecks the current PR head, grants and publication identity. Publishing is deliberately absent from this small example.
