package review

import (
	"fmt"
	"strings"
)

// instructions are sent in front of the first turn of a review session.
func instructions(request Request, pull Pull) string {
	var text strings.Builder
	text.WriteString("This is a protocol7 selfdev pull request review. No person is watching it live: your answers are read by a workflow.\n\n")
	fmt.Fprintf(&text, "The repository %s is cloned at /repo, checked out at the head of pull request #%d, commit %s.\n", request.Repo, request.Number, pull.Head)
	fmt.Fprintf(&text, "The pull request is %q by %s, from %s into %s.\n\n", pull.Title, pull.Author, pull.HeadRef, pull.Base)
	text.WriteString("You are the reviewer. You only read and judge:\n")
	text.WriteString("- Do not push, commit, comment on the pull request, change labels, or merge. The workflow publishes your verdict and decides about the merge.\n")
	text.WriteString("- Read the repository's AGENTS.md, README.md and docs/status.md, where they exist.\n")
	text.WriteString("- `gh pr view` and `gh pr diff` give the description and the diff. `git diff origin/" + pull.Base + "...HEAD` works in the clone.\n")
	text.WriteString("- Run the repository's own checks where you can, and say what you ran and what it showed. Do not claim a check passed that you did not run.\n")
	text.WriteString("- Look for what makes a change wrong: behaviour it breaks, a claim in a comment or document that the code does not support, a test that cannot fail, a secret or credential, a rule in the repository that it violates. Do not ask for style changes that no rule of the repository asks for.")
	return text.String()
}

func reviewPrompt(pull Pull) string {
	return fmt.Sprintf("Review pull request #%d. Say what you checked and what you found.", pull.Number)
}

// verdictPrompt is the second turn. Its answer is short, so that the verdict
// is not cut off the way the end of a long review could be.
const verdictPrompt = "Now give your verdict, and nothing else. Answer with one fenced block of this exact form:\n\n" +
	"```review-verdict\n" +
	"{\"verdict\": \"approve\", \"summary\": \"two or three sentences\", \"findings\": [\"one problem, with its file and line\"]}\n" +
	"```\n\n" +
	"verdict is approve, request_changes or comment. Use approve only if you would merge the change as it is. Use request_changes if something must change before it merges, and put each such thing in findings. Use comment if you cannot judge it. findings is empty for an approval."

const verdictRetryPrompt = "That was not a valid verdict. Answer again with the one fenced review-verdict block, and nothing else."

// reviewBody is the text of the GitHub review for a verdict.
func reviewBody(verdict Verdict, head string) string {
	var text strings.Builder
	text.WriteString(verdict.Summary)
	if len(verdict.Findings) > 0 {
		text.WriteString("\n\n")
		for _, finding := range verdict.Findings {
			text.WriteString("- " + strings.TrimSpace(finding) + "\n")
		}
	}
	fmt.Fprintf(&text, "\nReviewed at %s by the selfdev reviewer.\n%s%s:%s -->", short(head), ReviewMarker, head, verdict.Decision)
	return text.String()
}

func short(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}
