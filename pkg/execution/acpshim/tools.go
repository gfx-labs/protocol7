package acpshim

import (
	"context"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"strconv"

	acp "github.com/gfx-labs/go-acp"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

// toolServerID names the one MCP server a client offers over the ACP
// connection. It is unique on the connection because there is one.
const toolServerID = "p7-tools"

// mcpProtocolVersion is the MCP version the agent's ACP binding speaks, in
// which a server offered over ACP needs no initialize.
const mcpProtocolVersion = "2026-07-28"

// Tools are offered to the agent as an MCP server over the ACP connection.
type Tools interface {
	List() []domain.ToolSpec
	// Call runs one tool. callID is the same when the shim sends a request
	// again to a later connection, so a caller can run it once. The tools
	// fill in the call's session.
	Call(ctx context.Context, callID string, call domain.ToolCall) (domain.ToolResult, error)
}

// mcpServers is what a session is opened or loaded with.
func (c *Client) mcpServers() []acp.McpServer {
	if c.tools == nil {
		return []acp.McpServer{}
	}
	return []acp.McpServer{{Acp: &acp.McpServerAcpInline{Name: "p7", ServerId: toolServerID}}}
}

// UnstableMessageMcp answers one MCP request from the agent: tools/list,
// tools/call or ping. Other methods are refused with MCP's method not found.
func (a agentRequests) UnstableMessageMcp(ctx context.Context, request acp.UnstableMessageMcpRequest) (acp.UnstableMessageMcpResponse, error) {
	if a.tools == nil || request.ServerId != toolServerID {
		return acp.UnstableMessageMcpResponse{}, acp.NewInvalidParams(map[string]any{"error": fmt.Sprintf("no MCP server %q", request.ServerId)})
	}
	switch request.Method {
	case "ping":
		return mcpResult(map[string]any{}), nil
	case "initialize":
		return mcpResult(map[string]any{
			"protocolVersion": mcpProtocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "p7", "version": "0.0.0"},
		}), nil
	case "tools/list":
		var tools []any
		for _, spec := range a.tools.List() {
			schema := any(map[string]any{"type": "object"})
			if len(spec.InputSchema) > 0 {
				schema = jsontext.Value(spec.InputSchema)
			}
			tools = append(tools, map[string]any{"name": spec.Name, "description": spec.Description, "inputSchema": schema})
		}
		return mcpResult(map[string]any{"tools": tools}), nil
	case "tools/call":
		var params struct {
			Name      string         `json:"name"`
			Arguments jsontext.Value `json:"arguments"`
		}
		if err := remarshal(request.Params, &params); err != nil || params.Name == "" {
			return mcpError(-32602, "tools/call needs a tool name"), nil
		}
		call := domain.ToolCall{Tool: params.Name, Arguments: []byte(params.Arguments)}
		result, err := a.tools.Call(ctx, strconv.Itoa(a.shimPid)+"-"+string(request.RequestId), call)
		if err != nil {
			// MCP reports a tool that failed in its result, where the agent reads it.
			result = domain.ToolResult{Content: []domain.ToolContent{{Text: err.Error()}}, IsError: true}
		}
		content := []any{}
		for _, block := range result.Content {
			if block.MIMEType != "" {
				content = append(content, map[string]any{"type": "image", "data": block.Data, "mimeType": block.MIMEType})
			} else {
				content = append(content, map[string]any{"type": "text", "text": block.Text})
			}
		}
		return mcpResult(map[string]any{"content": content, "isError": result.IsError}), nil
	default:
		return mcpError(-32601, "method not found: "+request.Method), nil
	}
}

func mcpResult(result any) acp.UnstableMessageMcpResponse {
	return acp.UnstableMessageMcpResponse{Result: &acp.UnstableMessageMcpResult{Result: result}}
}

func mcpError(code int, message string) acp.UnstableMessageMcpResponse {
	return acp.UnstableMessageMcpResponse{Error: &acp.UnstableMessageMcpError{Error: acp.UnstableMcpError{Code: code, Message: message}}}
}

func remarshal(from, to any) error {
	content, err := jsonv2.Marshal(from)
	if err != nil {
		return err
	}
	return jsonv2.Unmarshal(content, to)
}
