package issue

import (
	"net/url"
	"strings"

	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
)

// chatLink is the address of the issue chat's page in navi, or "" when the
// template has no page address. An issue's chat has an owner, so navi lists it
// as a session, and it runs in the core project.
func chatLink(template Template, chatID string) string {
	if template.PageURL == "" || chatID == "" {
		return ""
	}
	return strings.TrimRight(template.PageURL, "/") + "/#/" + url.PathEscape(chat.CoreProject) + "/s/" + url.PathEscape(chatID)
}

// escalationLink is the address of an escalation's page in navi, where a
// person answers it, or "" when the template has no page address. The chat of
// an issue runs in the core project, and so do its escalations.
func escalationLink(template Template, escalationID string) string {
	if template.PageURL == "" || escalationID == "" {
		return ""
	}
	return strings.TrimRight(template.PageURL, "/") + "/#/" + url.PathEscape(chat.CoreProject) + "/e/" + url.PathEscape(escalationID)
}
