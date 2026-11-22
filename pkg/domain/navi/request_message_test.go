package navi

import (
	"strings"
	"testing"
)

// A follow-up request says which earlier request it continues, because it gets
// a chat of its own: the chat that handled the earlier one is not the one that
// answers it. The request's words stay in the fence either way.
func TestAFollowUpRequestMessageNamesTheEarlierRequest(t *testing.T) {
	origin := &ProjectOrigin{Project: "oku", Chat: "c1", Request: "r2"}
	request := &ProjectRequest{Need: "also {{.x}} ```", Follows: "r1"}
	text, err := RequestMessageText(origin, request)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "continues the earlier request `r1`") || !strings.Contains(text, "Answer this request in this chat") {
		t.Errorf("a follow-up does not name the earlier request:\n%s", text)
	}
	request.Follows = ""
	if text, err = RequestMessageText(origin, request); err != nil || strings.Contains(text, "earlier request") {
		t.Errorf("a first request says it continues one (%v):\n%s", err, text)
	}
}
