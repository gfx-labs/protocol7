package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

// WorkspacePath turns a path the agent gave into one inside the workspace
// dir: relative, with no "..". An absolute path is accepted when it is under
// dir, which is how the agent sees its own workspace on the host backend.
func WorkspacePath(dir, given string) (string, error) {
	given = strings.TrimSpace(given)
	if given == "" {
		return "", invalidf("the path is empty")
	}
	if strings.ContainsRune(given, 0) {
		return "", invalidf("the path holds a NUL byte")
	}
	if filepath.IsAbs(given) {
		relative, err := filepath.Rel(dir, filepath.Clean(given))
		if err != nil || !filepath.IsLocal(relative) {
			return "", invalidf("the path %q is not in the workspace", given)
		}
		given = relative
	}
	given = filepath.Clean(given)
	if !filepath.IsLocal(given) {
		return "", invalidf("the path %q is not in the workspace", given)
	}
	return filepath.ToSlash(given), nil
}

// OpenWorkspace opens the workspace of a session on the host backend, for a
// core worker whose state directory is stateDir. Everything done through the
// returned root stays in the workspace: it refuses "..", and a symbolic link
// that leads out.
func OpenWorkspace(stateDir, session string) (*os.Root, string, error) {
	dir, err := p7.HostWorkspace(stateDir, session)
	if err != nil {
		return nil, "", invalidf("no such session %q", session)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, "", invalidf("the workspace of session %q cannot be opened", session)
	}
	return root, dir, nil
}

// SaveResult is what a save made.
type SaveResult struct {
	Project string `json:"project"`
	Name    string `json:"name"`
	// Ref is p7://sha256:<hex>#<name>.
	Ref     string `json:"ref"`
	Digest  string `json:"digest"`
	Size    int64  `json:"size"`
	Dir     bool   `json:"dir"`
	Changed bool   `json:"changed"`
}

// SaveFrom saves the file or directory at relative path p of root, under name
// in project. A directory is saved as a zip.
func (s *Store) SaveFrom(ctx context.Context, root *os.Root, p, project, name, by string) (SaveResult, error) {
	if err := ValidProject(project); err != nil {
		return SaveResult{}, err
	}
	if err := ValidName(name); err != nil {
		return SaveResult{}, err
	}
	info, err := root.Stat(p)
	if err != nil {
		return SaveResult{}, fmt.Errorf("%s: %w", p, workspaceError(err))
	}
	before, lookupErr := s.Lookup(ctx, project, name)
	if lookupErr != nil && !errors.Is(lookupErr, ErrNotFound) {
		return SaveResult{}, lookupErr
	}
	var version Version
	result := SaveResult{Project: project, Name: name}
	switch {
	case info.IsDir():
		dir, err := fs.Sub(root.FS(), p)
		if p == "." {
			dir, err = root.FS(), nil
		}
		if err != nil {
			return SaveResult{}, err
		}
		reader, writer := io.Pipe()
		go func() { writer.CloseWithError(zipDir(writer, dir, s.maxSize())) }()
		version, err = s.Save(ctx, project, name, by, reader)
		reader.CloseWithError(err)
		result.Dir = true
		if err != nil {
			return SaveResult{}, err
		}
	case info.Mode().IsRegular():
		if info.Size() > s.maxSize() {
			return SaveResult{}, fmt.Errorf("%s is larger than %d MiB: %w", p, s.maxSize()>>20, ErrTooLarge)
		}
		file, err := root.Open(p)
		if err != nil {
			return SaveResult{}, fmt.Errorf("%s: %w", p, workspaceError(err))
		}
		defer file.Close()
		version, err = s.Save(ctx, project, name, by, file)
		if err != nil {
			return SaveResult{}, err
		}
	default:
		return SaveResult{}, invalidf("%s is neither a file nor a directory", p)
	}
	result.Ref = version.Ref(name).String()
	result.Digest, result.Size = version.Digest, version.Size
	result.Changed = lookupErr != nil || before.Digest != version.Digest
	return result, nil
}

// GetResult is what a get placed in the workspace.
type GetResult struct {
	Ref string `json:"ref"`
	// Path is where it is, relative to the workspace.
	Path string `json:"path"`
	Size int64  `json:"size"`
	// Files is how many files a directory held. A file is 1.
	Files int  `json:"files"`
	Dir   bool `json:"dir"`
}

// GetTo places an artifact in root at relative path p. A zip that a directory
// was saved as is unpacked under p. A file is written to p, or into p when
// that is a directory, under the base of its name. The bytes are checked
// against their digest before anything is written.
func (s *Store) GetTo(ctx context.Context, root *os.Root, p string, ref Ref) (GetResult, error) {
	file, cleanup, err := s.Fetch(ctx, ref.Digest)
	if err != nil {
		return GetResult{}, err
	}
	defer cleanup()
	result := GetResult{Ref: ref.String(), Path: p}
	if archive, ok := openDirArchive(file); ok {
		if info, err := root.Stat(p); err == nil && !info.IsDir() {
			return GetResult{}, invalidf("%s is a file, and the artifact is a directory", p)
		}
		files, size, err := unzip(root, p, archive, s.maxSize())
		if err != nil {
			return GetResult{}, fmt.Errorf("unpack into %s: %w", p, err)
		}
		result.Dir, result.Files, result.Size = true, files, size
		return result, nil
	}
	if info, err := root.Stat(p); err == nil && info.IsDir() {
		base := path.Base(ref.Name)
		if ref.Name == "" {
			base = ref.Digest
		}
		p = path.Join(p, base)
		result.Path = p
	}
	if dir := path.Dir(p); dir != "." {
		if err := root.MkdirAll(dir, 0o755); err != nil {
			return GetResult{}, fmt.Errorf("%s: %w", p, workspaceError(err))
		}
	}
	size, err := copyFile(root, p, file, false, s.maxSize())
	if err != nil {
		return GetResult{}, fmt.Errorf("write %s: %w", p, workspaceError(err))
	}
	result.Size, result.Files = size, 1
	return result, nil
}

// workspaceError tells the agent what a refusal of the root means.
func workspaceError(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return invalidf("does not exist")
	}
	if strings.Contains(err.Error(), "path escapes from parent") {
		return invalidf("is not in the workspace: it climbs out of it, or goes through a symbolic link that does")
	}
	return err
}
