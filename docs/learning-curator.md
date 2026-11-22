# The learnings curator

Status: built in the SDK (`pkg/orchestration/learning`, `curate.go`, `curateactivities.go`, `lanes.go`, `schedule.go`) and in protocol7-selfdev (`pkg/workflow/curator`). oku-review's part is specified here and waits for that project (see "oku-review"). Written 2026-11-22.

Chats record what they learned with `learning_record`. Nobody accepts a learning by hand any more: a curator turns learnings into changes. It is modelled on jobcity's curate pass, with the mechanics in the SDK and the words, the lanes and the tools in each app.

## Behavior

- **A learning is open until the curator settles it.** A recorded learning is `open` and every chat of the project reads it (the first prompt's brief and `learning_search`) from then on. Learnings from before the curator are `proposed` (the same as open) or `accepted` (read as before).
- **A pass.** A Temporal schedule per namespace (`learning-curate`, overlap policy skip) starts `selfdev.LearningCurate` every few hours (default 4). The pass checks, then runs one chat of the project, the curator's session, for one turn. The chat has the project's repositories, its scope and its normal pull request tools, and the app's curator tools. The pass closes the chat, reads back what became of each entry and keeps the curator's last message as the pass note.
- **When a pass opens.** At least one open entry was recorded since the last pass that ran, and no other pass runs. A manual run (`learning.StartPass`) needs only an open entry. A pass that skipped, or failed, is not "the last pass": a failed pass's entries are taken again at the next run. When more entries are open than one pass takes (`MaxEntries`, 30, oldest first), the cursor stops at the last one given, so the rest count as new.
- **What the curator does.** It groups the open entries by cause (the app's brief says how, with dedupe and embedding families where the app has them), checks each claim with one command against the current code, and makes one bounded edit per group in the narrowest owner: a skill, a prompt or guidance file, a doc, or a tool fix with its test. It settles every entry with `learning_decide`:
  - **applied**: a change carries it. The entry links the pull request.
  - **covered**: a line of the code or the docs says it already. The note names the line. The entry ends.
  - **rejected**: wrong, a one-off or not general. The note says why. The entry ends.
  - **left open**: not enough evidence yet. It stays open to join a later recurrence, and the pass note says so.
  The curator never weakens a review, merge, CI, credential or production gate, and never edits the gate that checks its own work.
- **Two lanes, three kinds of change.** The app lists globs (`learning.Lanes`). A change whose files all match the fast globs (skills, ops docs, notes) is *fast*: the curator merges it itself, after its checks pass, through the app's guarded merge tool. Any other change is *reviewed*: it goes through the app's normal review, which merges it. A change that touches a protected path needs a person. `learning.Protected` (`.github/**`, `CODEOWNERS`) is never fast whatever an app lists. The guard is in the merge tool's activity, which reads the pull request's files from GitHub and classifies them. The prompt tells the curator the same, but it is not what stops it.
- **Override.** navi's Learnings page lists the entries with their state, PR and note. A person may reject an open entry and retire an accepted or applied one. There is nothing to accept.

## States

| State | Meaning | Workflow | Chats read it |
|---|---|---|---|
| `open` (`proposed` from before) | recorded, not settled | runs | yes |
| `applied` | a change carries it (`pr`) | runs, until retired | yes |
| `accepted` | from before the curator | runs | yes |
| `covered` | a line says it already (`note`) | ends | no |
| `rejected` | not kept (`note`) | ends | no |
| `retired` | a person withdrew an accepted or applied entry | ends | no |

A decision that settles an entry (`apply`, `cover`, `reject`) rewrites the entry's memo with the PR, the note, the pass, who decided and when, so a listing shows them. A covered, rejected or retired entry ages out with Temporal's retention (seven days); an open or applied one is not removed. The curator's pass keeps its own result in its memo (`learning_curate_pass`), so a pass is listed with the entries it settled and its note.

## What the SDK holds

- The states, the actions (`accept`, `reject`, `retire`, `apply`, `cover`), `Entry.Settle` and `CheckDecision`: an apply needs an https PR link, a cover needs a note, a note holds no secret.
- The learning workflow with the settle update, and `Store.Open`, `Store.ListAll`.
- `CurateWorkflow`, `CurateActivities` (`LearningCurateCheck`, `LearningCurateOutcome`), `RegisterCurate`, `EnsureSchedule`, `StartPass`, `CurateConfig` (`Every`, `MinNew`, `MaxEntries`, `TurnBudget`), `IsCuratorChat`.
- `Lanes`, `Match`, `Protected`.
- `learningtest`, the in-memory log.

It holds no prompt text and no tool. The pass calls the app's activity `LearningCurateBrief` (`BriefRequest` in, `Brief{Title, Message}` out) for the curator's first message.

## What an app provides

1. **The session.** The `navi.ChatRequest` the curator's chat starts from: host, backend, models, repositories, scope and project. The app builds it when its worker starts and gives it to `EnsureSchedule` with `CurateConfig`. The pass sets the ID, the title, the owner (the pass, so that `IsCuratorChat` is true) and clears the handler, parent and request targets.
2. **The brief.** `LearningCurateBrief`: the instructions of a pass (the method above, the lanes in words, the settle rules, how to write the pass note) and the entries, fenced, because they are another chat's words. Built with `pkg/support/prompt`.
3. **The tools of a curator chat** (`IsCuratorChat`): the pull request tools, `learning_search`, `learning_decide` with `apply`, `cover` and `reject`, and a guarded merge for the fast lane. Not the tools that reach production or spawn work beyond the pass.
4. **The lanes** (`learning.Lanes`), and a merge activity that classifies the pull request's files with them and refuses anything but a fast change that is open, not a draft, mergeable and whose checks all passed.
5. **The wiring.** `learning.RegisterCurate`, the brief activity, `EnsureSchedule` on start, and the settings (`--curate-every`, none by default means no schedule).

## selfdev

Fast: `docs/**/*.md`, `README.md` and `AGENTS.md` at any depth, `skills/**`, `kubernetes/skills/**`. Reviewed: code, prompts (`pkg/**/prompts/*.md`), tools and every other path, through `selfdev review`. Person: `.github/**`, `CODEOWNERS`, and the policy and grant files under `kubernetes/` that the app lists. See protocol7-selfdev's README, "Learnings curator".

## oku-review

oku-review builds its own worker from its own repository against a pinned protocol7, so its curator is written there, with the same parts: guidance markdown and docs are fast, the protected review paths of oku-review need a person, and the rest goes through reviewer-a's self-dev review. This repository cannot reach oku-trade's repositories, so the work is a request to that project, which needs oku-review's own paths. The request, ready to send with `project_request` (target `oku-review`) from a chat that may ask it:

> **Need:** wire the learnings curator of protocol7 `docs/learning-curator.md` into oku-review's worker, on protocol7 at the commit that holds `pkg/orchestration/learning` curate.go (the pull request that added it merged to master). **Why:** learnings no longer need a person to accept them, and oku-review's log is read by its chats and settled by nobody. **Do:** (1) bump protocol7. (2) `learning.RegisterCurate` on the worker, and register `LearningCurateBrief` with oku-review's brief (adapt `pkg/workflow/curator/prompts` of protocol7-selfdev). (3) `learning.EnsureSchedule` at start with the session of an oku-review chat and `CurateConfig{Every: 4h}`. (4) the curator's tools for `learning.IsCuratorChat`: `learning_decide` with apply, cover and reject, and the guarded merge with oku-review's lanes: fast is guidance markdown and docs, person is the protected review paths (the ones its review flow already refuses to let an agent merge), and reviewed is the rest, which the curator hands to reviewer-a's self-dev review and does not merge. (5) replay test of a history recorded before this change, and a test of the lanes. **Next:** I will run a live pass on its open learnings and report the outcomes.

## Tests

`pkg/orchestration/learning`: the states and `Settle`, the workflow's decisions (a cover, a rejection, an apply then retire, the memo), the check (nothing open, nothing new, an earlier pass running, the last pass found among skipped ones, a manual run, the backlog cut and its cursor), the pass over Temporal's test environment with a stub chat (a skip, a run that reports its turn and closes the chat, the chat's request, the budget, no note, a failed turn), the schedule's create and update, and the lanes. `TestLiveLearningsReplay` replays a recorded entry on the changed workflow.

## Not built

- A reviewed-lane pull request that is closed unmerged leaves its entry applied. A later pass could look at applied entries whose PR was closed and reopen them.
- No page action starts a manual pass. `learning.StartPass` is the function.
