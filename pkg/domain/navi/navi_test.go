package navi_test

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"
)

// chatView is the envelope navi's chat endpoint wraps a ChatState in.
type chatView struct {
	ID    string         `json:"id"`
	State navi.ChatState `json:"state"`
}

// TestChatStateDecodesLiveQuery decodes the state a running chat returned to
// navi's query (captured from navi's /api/projects/selfdev/chats endpoint).
// An unknown field fails the decode, so a field the workflow sends that the
// contract lacks shows up here.
func TestChatStateDecodesLiveQuery(t *testing.T) {
	recorded, err := os.ReadFile("testdata/chat-state.json")
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(recorded))
	decoder.DisallowUnknownFields()
	var view chatView
	if err := decoder.Decode(&view); err != nil {
		t.Fatalf("decode the recorded chat state: %v", err)
	}
	state := view.State
	if state.Request.ID != view.ID || state.Status != navi.ChatIdle || state.Request.Kind() != navi.KindChat {
		t.Errorf("request %q status %q kind %q", state.Request.ID, state.Status, state.Request.Kind())
	}
	if state.Request.Scope == nil || !state.Request.Scope.Allows("contents", "write") {
		t.Errorf("scope = %+v, want contents write", state.Request.Scope)
	}
	if state.Session == nil || len(state.Session.Inputs) == 0 || state.Session.TaskQueue == "" {
		t.Errorf("session = %+v", state.Session)
	}
	if !state.Result.Idle || state.Result.ChatID != view.ID {
		t.Errorf("result = %+v", state.Result)
	}

	// Encoding the decoded state gives back the recorded JSON, so no field is
	// lost on the way through the contract.
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var want, got any
	if err := json.Unmarshal(recorded, &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	wantText, _ := json.Marshal(want)
	gotText, _ := json.Marshal(got)
	if !bytes.Equal(wantText, gotText) {
		t.Errorf("round trip changed the state:\nwant %s\ngot  %s", wantText, gotText)
	}
}
