// Package linear calls Linear's GraphQL API.
//
// The client holds no key. It sends a PDP placeholder in the Authorization
// header, and PDP puts the Linear app's token in its place by the identity of
// the pod (kubernetes/pdp, linear_binding).
package linear

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// DefaultURL is Linear's GraphQL endpoint.
const DefaultURL = "https://api.linear.app/graphql"

// Client sends GraphQL requests to Linear.
type Client struct {
	// URL is the GraphQL endpoint. Empty is DefaultURL.
	URL string
	// Authorization is sent as the Authorization header as it is, with no
	// "Bearer" in front: Linear takes an API key that way, and PDP matches the
	// placeholder there.
	Authorization string
	// HTTP sends the requests. Nil is http.DefaultClient.
	HTTP *http.Client
}

// StatusError is a response whose status was not a success.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("Linear answered %d: %s", e.Code, e.Body)
}

// Retryable says whether trying again may help.
func (e *StatusError) Retryable() bool {
	return e.Code >= 500 || e.Code == http.StatusTooManyRequests
}

// GraphQLError is a response that carried errors, for example an unknown
// issue or a mutation that Linear refused.
type GraphQLError struct {
	Messages []string
}

func (e *GraphQLError) Error() string {
	return "Linear refused the request: " + strings.Join(e.Messages, "; ")
}

// errorBodyBytes bounds the body quoted in a StatusError.
const errorBodyBytes = 500

// Do sends one query or mutation with its variables and decodes the response's
// data into result, which may be nil.
func (c *Client) Do(ctx context.Context, query string, variables map[string]any, result any) error {
	encoded, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return fmt.Errorf("encode the Linear request: %w", err)
	}
	endpoint := c.URL
	if endpoint == "" {
		endpoint = DefaultURL
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("make the Linear request: %w", err)
	}
	request.Header.Set("Authorization", c.Authorization)
	request.Header.Set("Content-Type", "application/json")
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("call Linear: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return fmt.Errorf("read Linear's response: %w", err)
	}
	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	decodeErr := json.Unmarshal(body, &envelope)
	if len(envelope.Errors) > 0 {
		messages := make([]string, 0, len(envelope.Errors))
		for _, failure := range envelope.Errors {
			messages = append(messages, failure.Message)
		}
		return &GraphQLError{Messages: messages}
	}
	if response.StatusCode >= 300 {
		text := strings.TrimSpace(string(body))
		if len(text) > errorBodyBytes {
			text = text[:errorBodyBytes]
		}
		return &StatusError{Code: response.StatusCode, Body: text}
	}
	if decodeErr != nil {
		return fmt.Errorf("decode Linear's response: %w", decodeErr)
	}
	if result == nil {
		return nil
	}
	if err := json.Unmarshal(envelope.Data, result); err != nil {
		return fmt.Errorf("decode Linear's data: %w", err)
	}
	return nil
}
