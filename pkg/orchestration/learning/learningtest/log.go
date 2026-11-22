// Package learningtest is an in-memory learnings log behind a Temporal client
// mock, so that tests of an app's tools, prompts and navi's page run against the
// real Store.
package learningtest

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/temporal"

	"github.com/gfx-labs/protocol7/pkg/orchestration/learning"
)

// Log keeps the entries whose workflow would run.
type Log struct {
	Client  *mocks.Client
	mu      sync.Mutex
	entries []learning.Entry
	// closed are the entries whose workflow ended: covered, rejected and
	// retired. Get and the listings of every status still see them, as
	// Temporal's retention lets them.
	closed []learning.Entry
	// Queries are the visibility queries that were asked.
	Queries []string
}

// New makes an empty log.
func New(t testing.TB) *Log {
	l := &Log{Client: &mocks.Client{}}
	data := converter.GetDefaultDataConverter()
	l.Client.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		func(_ context.Context, options client.StartWorkflowOptions, _ any, args ...any) (client.WorkflowRun, error) {
			l.mu.Lock()
			defer l.mu.Unlock()
			entry := args[0].(learning.Entry)
			for _, have := range l.entries {
				if learning.WorkflowID(have.ID) == options.ID {
					return nil, serviceerror.NewWorkflowExecutionAlreadyStarted("started", "", "")
				}
			}
			l.entries = append(l.entries, entry)
			return &mocks.WorkflowRun{}, nil
		})
	l.Client.On("ListWorkflow", mock.Anything, mock.Anything).Return(
		func(_ context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.Queries = append(l.Queries, request.GetQuery())
			response := &workflowservice.ListWorkflowExecutionsResponse{}
			listed := l.entries
			if !strings.Contains(request.GetQuery(), "ExecutionStatus") {
				listed = append(append([]learning.Entry(nil), l.entries...), l.closed...)
			}
			for _, entry := range listed {
				memo, err := data.ToPayload(entry)
				if err != nil {
					return nil, err
				}
				state, err := data.ToPayload(string(entry.State))
				if err != nil {
					return nil, err
				}
				response.Executions = append(response.Executions, &workflowpb.WorkflowExecutionInfo{
					Execution:        &commonpb.WorkflowExecution{WorkflowId: learning.WorkflowID(entry.ID)},
					Memo:             &commonpb.Memo{Fields: map[string]*commonpb.Payload{learning.MemoEntry: memo}},
					SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{learning.AttributeState.GetName(): state}},
				})
			}
			return response, nil
		})
	l.Client.On("DescribeWorkflowExecution", mock.Anything, mock.Anything, mock.Anything).Return(
		func(_ context.Context, id, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
			l.mu.Lock()
			defer l.mu.Unlock()
			for _, entry := range append(append([]learning.Entry(nil), l.entries...), l.closed...) {
				if learning.WorkflowID(entry.ID) != id {
					continue
				}
				memo, err := data.ToPayload(entry)
				if err != nil {
					return nil, err
				}
				state, err := data.ToPayload(string(entry.State))
				if err != nil {
					return nil, err
				}
				return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Execution:        &commonpb.WorkflowExecution{WorkflowId: id},
					Memo:             &commonpb.Memo{Fields: map[string]*commonpb.Payload{learning.MemoEntry: memo}},
					SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{learning.AttributeState.GetName(): state}},
				}}, nil
			}
			return nil, serviceerror.NewNotFound("no such workflow")
		})
	l.Client.On("UpdateWorkflow", mock.Anything, mock.Anything).Return(
		func(_ context.Context, options client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
			l.mu.Lock()
			defer l.mu.Unlock()
			decision := options.Args[0].(learning.Decision)
			for i, entry := range l.entries {
				if learning.WorkflowID(entry.ID) != options.WorkflowID {
					continue
				}
				entry, err := entry.Settle(decision, time.Now())
				if err != nil {
					return nil, temporal.NewApplicationError(err.Error(), "")
				}
				handle := &mocks.WorkflowUpdateHandle{}
				handle.On("Get", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
					// As Temporal does, decode the result into whatever type the caller holds.
					payload, err := converter.GetDefaultDataConverter().ToPayload(entry)
					if err == nil {
						err = converter.GetDefaultDataConverter().FromPayload(payload, args.Get(1))
					}
					if err != nil {
						panic(err)
					}
				})
				if entry.State.Ended() {
					l.entries = append(l.entries[:i], l.entries[i+1:]...)
					l.closed = append(l.closed, entry)
				} else {
					l.entries[i] = entry
				}
				return handle, nil
			}
			return nil, serviceerror.NewNotFound("no such workflow")
		})
	return l
}

// Entries are the entries now in the log.
func (l *Log) Entries() []learning.Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]learning.Entry(nil), l.entries...)
}

// Store is the real store over the log.
func (l *Log) Store() learning.Store { return learning.Store{Client: l.Client} }

// Activities are the real activities over the log.
func (l *Log) Activities() *learning.Activities { return &learning.Activities{Store: l.Store()} }

// Closed are the entries whose workflow ended.
func (l *Log) Closed() []learning.Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]learning.Entry(nil), l.closed...)
}

// Add puts an entry in the log as it is, in the state it holds, so that a
// test starts from entries a curator has seen. An entry in an ended state
// goes with the closed ones.
func (l *Log) Add(entry learning.Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if entry.State.Ended() {
		l.closed = append(l.closed, entry)
		return
	}
	l.entries = append(l.entries, entry)
}
