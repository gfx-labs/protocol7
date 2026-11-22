package navi

import (
	_ "embed"
	"fmt"
	"strings"

	"github.com/gfx-labs/protocol7/pkg/support/prompt"
)

//go:embed request-message.md
var requestMessageTemplate string

//go:embed request-reply.md
var requestReplyTemplate string

// RequestMessageText is the first message of the chat that works on a request
// in the target project. It names the project that asked, and holds the request's words in a
// fence, as untrusted input that cannot run as a template or close the block.
func RequestMessageText(origin *ProjectOrigin, request *ProjectRequest) (string, error) {
	var asked strings.Builder
	for _, part := range []struct{ name, text string }{
		{"Needed", request.GetNeed()}, {"Why the requester cannot do it", request.GetWhy()},
		{"What it tried", request.GetTried()}, {"What it will do with the answer", request.GetNext()},
	} {
		if text := strings.TrimSpace(part.text); text != "" {
			fmt.Fprintf(&asked, "%s:\n%s\n\n", part.name, text)
		}
	}
	text := prompt.New(prompt.Vars{
		"Origin": origin.GetProject(), "Chat": origin.GetChat(), "Request": origin.GetRequest(), "Follows": request.GetFollows(),
	}).Template(requestMessageTemplate).
		Fence("text", strings.TrimSpace(asked.String())).
		P("Answer it with the tool `project_request_reply`, as your instructions say.")
	if err := text.Err(); err != nil {
		return "", err
	}
	return text.String(), nil
}

// ReplyMessageText is the message that carries a target's answer to the chat
// that asked. The answer's words are in a fence: they come from an agent of
// another project.
func ReplyMessageText(ref *ProjectRequestRef, reply *ProjectReply) (string, error) {
	meaning := map[string]string{
		ReplyAccepted:   "The project took the work. It says below what it will deliver. You get another message when it is done: go on with what you can do meanwhile, and do not ask again.",
		ReplyRedirected: "The project did not take the work and says what to do instead. Do that, and do not send the same request again.",
		ReplyDeclined:   "The project declined, with the reason below. Do not do what you asked it to do yourself outside your reach, and do not send the same request again. Say in your last message of the turn what is left undone.",
		ReplyDone:       "The project delivered what it accepted. Check it, and go on with what you said you would do with the answer.",
	}[reply.GetOutcome()]
	var body strings.Builder
	body.WriteString(strings.TrimSpace(reply.GetText()))
	if reference := strings.TrimSpace(reply.GetReference()); reference != "" {
		body.WriteString("\n\nReference: " + reference)
	}
	text := prompt.New(prompt.Vars{
		"Target": ref.GetTarget(), "Request": ref.GetId(), "Title": ref.GetTitle(), "Outcome": reply.GetOutcome(), "Meaning": meaning,
	}).Template(requestReplyTemplate).Fence("text", body.String())
	if err := text.Err(); err != nil {
		return "", err
	}
	return text.String(), nil
}
