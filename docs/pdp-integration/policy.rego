# protocol7 run policy, proposed. The caller's SPIFFE ID carries the run's
# profile and repository, so this policy needs no per-run data and no reload
# when a run starts.
#
#   spiffe://<trust domain>/p7/profile/<profile>/repo/<owner>/<repo>/run/<run id>
package pdp.authz

import rego.v1

run_prefix := "spiffe://p7.example/p7/"

default allow := false

allow if model_call

allow if model_list

allow if git_fetch

# Undefined for any caller that is not a protocol7 run, so every rule below
# that reads it is false for such a caller.
run := parsed if {
	input.identity.auth_method == "spiffe"
	startswith(input.identity.spiffe_id, run_prefix)
	parts := split(trim_prefix(input.identity.spiffe_id, run_prefix), "/")
	count(parts) == 7
	parts[0] == "profile"
	parts[2] == "repo"
	parts[5] == "run"
	parsed := {"profile": parts[1], "owner": parts[3], "repo": parts[4], "id": parts[6]}
}

# grants.yaml: what each profile may do.
profile := data.profiles[run.profile]

model_paths := {
	"api.anthropic.com": {"/v1/messages"},
	"chatgpt.com": {"/backend-api/codex/responses", "/backend-api/codex/responses/compact"},
}

default model_call := false

# input.binding is the account the request's placeholder named. The profile
# must grant it: knowing another account's placeholder grants nothing.
model_call if {
	input.protocol == "http"
	input.destination.scheme == "https"
	input.destination.port == 443
	input.http.method == "POST"
	input.http.path in model_paths[input.destination.host]
	input.binding in profile.model_accounts
}

model_list_paths := {
	"api.anthropic.com": {"/v1/models"},
	"chatgpt.com": {"/backend-api/codex/models"},
}

default model_list := false

# towa reads an account's model list at start, for the context size and output
# limit of a model its built-in table does not have. Without it and without
# both figures in its configuration, the session does not start.
model_list if {
	input.protocol == "http"
	input.destination.scheme == "https"
	input.destination.port == 443
	input.http.method == "GET"
	input.http.path in model_list_paths[input.destination.host]
	input.binding in profile.model_accounts
}

# A clone URL may be written with or without ".git".
repo_paths := {
	sprintf("/%s/%s", [run.owner, run.repo]),
	sprintf("/%s/%s.git", [run.owner, run.repo]),
}

default git_destination := false

git_destination if {
	input.protocol == "http"
	input.destination.scheme == "https"
	input.destination.host == "github.com"
	input.destination.port == 443
	input.binding == "github-read"
}

default git_fetch := false

# Smart HTTP fetch is two requests. A push uses git-receive-pack in both
# places, which no rule here allows.
git_fetch if {
	profile.git == "read"
	git_destination
	input.http.method == "GET"
	some base in repo_paths
	input.http.path == concat("", [base, "/info/refs"])
	input.http.query.service == ["git-upload-pack"]
}

git_fetch if {
	profile.git == "read"
	git_destination
	input.http.method == "POST"
	some base in repo_paths
	input.http.path == concat("", [base, "/git-upload-pack"])
}

matched_rules contains sprintf("model:%s", [input.binding]) if model_call

matched_rules contains sprintf("model-list:%s", [input.binding]) if model_list

matched_rules contains "git-fetch" if git_fetch

reasons contains "caller is not a protocol7 run" if not run

reasons contains "no rule allows this run, destination and request" if {
	run
	not allow
}
