// Package session runs agent sessions on the host backend: one directory per
// session that holds its workspace, its shim and its record. Manager opens a
// session, runs turns in it and closes it. Each call can be repeated: it
// reads the session's record and journal to find what an earlier attempt
// already did, so a worker that restarts during a turn picks the turn up
// where it is.
package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

const (
	recordFile   = "session.json"
	shimDirName  = "shim"
	workspaceDir = "work"
)

// record is what is kept about a session between activities.
type record struct {
	ID      string                 `json:"id"`
	Request domain.SessionRequest  `json:"request"`
	Inputs  []domain.ResolvedInput `json:"inputs,omitempty"`
	// ACPSession is the harness's ID of the conversation.
	ACPSession string `json:"acp_session,omitempty"`
	// ShimPid is the shim whose harness has ACPSession open. Another shim
	// runs a new harness process, which has to load the conversation first.
	ShimPid int  `json:"shim_pid,omitempty"`
	Closed  bool `json:"closed,omitempty"`
}

// The ID is a directory name and part of a unix socket path, which Linux
// limits to 107 bytes.
var sessionID = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,47}$`)

func checkID(id string) error {
	if !sessionID.MatchString(id) {
		return fmt.Errorf("session ID %q: use up to 48 lower-case letters, digits, '.', '_' or '-', starting with a letter or digit", id)
	}
	return nil
}

func (m *Manager) sessionDir(id string) string { return filepath.Join(m.config.Root, "sessions", id) }

func (m *Manager) shimDir(id string) string { return filepath.Join(m.sessionDir(id), shimDirName) }

func (m *Manager) workspace(id string) string { return filepath.Join(m.sessionDir(id), workspaceDir) }

// load reads the record of a session. A session that was never opened is an
// error wrapping os.ErrNotExist.
func (m *Manager) load(id string) (record, error) {
	var rec record
	if err := checkID(id); err != nil {
		return rec, err
	}
	content, err := os.ReadFile(filepath.Join(m.sessionDir(id), recordFile))
	if err != nil {
		return rec, fmt.Errorf("read record of session %s: %w", id, err)
	}
	if err := json.Unmarshal(content, &rec); err != nil {
		return rec, fmt.Errorf("decode record of session %s: %w", id, err)
	}
	return rec, nil
}

// save replaces the record by rename, so an interrupted write leaves the
// earlier record.
func (m *Manager) save(rec record) error {
	content, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("encode record of session %s: %w", rec.ID, err)
	}
	path := filepath.Join(m.sessionDir(rec.ID), recordFile)
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, append(content, '\n'), 0o600); err != nil {
		return fmt.Errorf("write record of session %s: %w", rec.ID, err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("replace record of session %s: %w", rec.ID, err)
	}
	return nil
}

func isMissing(err error) bool { return errors.Is(err, os.ErrNotExist) }
