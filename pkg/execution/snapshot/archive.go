package snapshot

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// entry is one thing the walk found, to be written to the archive.
type entry struct {
	// name is the path relative to the root, with "/" separators. "." is
	// the root itself.
	name string
	info fs.FileInfo
	// link is the target of a symbolic link.
	link string
}

// tree is what a walk found: what an archive is written from, and its
// fingerprint.
type tree struct {
	root    string
	entries []entry
	// Fingerprint identifies the content that the walk saw: names, types,
	// modes, sizes, modification times and link targets. A tree that is
	// unchanged has the same one.
	fingerprint string
	// skipped lists what is left out for being neither a regular file, a
	// directory nor a link that stays inside the root.
	skipped []string
}

// excluded reports whether the entry at name is left out by patterns. A
// pattern with no slash matches the name of an entry at any depth. One with
// a slash matches the path from the root. Both use path.Match syntax. A
// directory that is left out is left out with everything in it.
func excluded(name string, patterns []string) bool {
	for _, pattern := range patterns {
		pattern = strings.Trim(pattern, "/")
		if pattern == "" {
			continue
		}
		subject := name
		if !strings.Contains(pattern, "/") {
			subject = path.Base(name)
		}
		if ok, _ := path.Match(pattern, subject); ok {
			return true
		}
	}
	return false
}

// walk reads the tree under root. Regular files, directories and symbolic
// links are kept. Sockets, devices, pipes and links that point out of the
// root are skipped.
func walk(root string, exclude []string) (*tree, error) {
	t := &tree{root: root}
	err := filepath.WalkDir(root, func(full string, dirEntry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && full != root {
				// Removed while the walk ran.
				return nil
			}
			return err
		}
		relative, err := filepath.Rel(root, full)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(relative)
		if name != "." && excluded(name, exclude) {
			if dirEntry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := dirEntry.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		switch {
		case info.Mode().IsRegular(), info.IsDir():
			t.entries = append(t.entries, entry{name: name, info: info})
		case info.Mode()&fs.ModeSymlink != 0:
			target, err := os.Readlink(full)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				}
				return err
			}
			if linkEscapes(name, target) {
				t.skipped = append(t.skipped, name+" -> "+target)
				return nil
			}
			t.entries = append(t.entries, entry{name: name, info: info, link: target})
		default:
			t.skipped = append(t.skipped, name)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", root, err)
	}
	hash := sha256.New()
	for _, e := range t.entries {
		// A directory's size and modification time change with its content,
		// which is listed, and a link's time cannot be set again on restore.
		var modified, size int64
		if e.info.Mode().IsRegular() {
			modified, size = e.info.ModTime().UnixNano(), e.info.Size()
		}
		fmt.Fprintf(hash, "%s\x00%o\x00%d\x00%d\x00%s\n", e.name, e.info.Mode(), size, modified, e.link)
	}
	t.fingerprint = hex.EncodeToString(hash.Sum(nil))
	return t, nil
}

// linkEscapes reports whether a symbolic link at name with the given target
// may point outside the root, or cannot be shown not to. An absolute target
// does. A relative one stays inside if its leading ".." elements do not
// climb above the root, and it has no ".." after another element, because
// that climbs out of a link, not out of a name, and where it ends depends on
// the links on the way.
func linkEscapes(name, target string) bool {
	if target == "" || strings.HasPrefix(target, "/") || strings.ContainsRune(target, 0) {
		return true
	}
	depth := strings.Count(name, "/")
	leading := true
	for element := range strings.SplitSeq(target, "/") {
		switch {
		case element == "..":
			if !leading {
				return true
			}
			depth--
			if depth < 0 {
				return true
			}
		case element == "." || element == "":
		default:
			leading = false
		}
	}
	return false
}

// writeArchive writes t as tar in PAX format, compressed with zstd at its
// default level, to w. It streams: nothing is held but the file being copied.
//
// A file that grows after the walk is cut at the size the walk saw, and one
// that shrinks is padded with zeros, so each entry is as long as its header
// says. The next walk sees the change.
func writeArchive(w io.Writer, t *tree) error {
	compressor, err := zstd.NewWriter(w, zstd.WithEncoderConcurrency(2))
	if err != nil {
		return err
	}
	archive := tar.NewWriter(compressor)
	for _, e := range t.entries {
		if err := writeEntry(archive, t.root, e); err != nil {
			_ = compressor.Close()
			return err
		}
	}
	if err := archive.Close(); err != nil {
		_ = compressor.Close()
		return fmt.Errorf("finish tar: %w", err)
	}
	if err := compressor.Close(); err != nil {
		return fmt.Errorf("finish zstd: %w", err)
	}
	return nil
}

func writeEntry(archive *tar.Writer, root string, e entry) error {
	header, err := tar.FileInfoHeader(e.info, e.link)
	if err != nil {
		return fmt.Errorf("header of %s: %w", e.name, err)
	}
	header.Name = e.name
	if e.info.IsDir() && e.name != "." {
		header.Name += "/"
	}
	// Only the permission bits travel. Owners do not: a snapshot is restored
	// by whichever user the new worker runs as.
	header.Mode = int64(e.info.Mode().Perm())
	header.Uid, header.Gid, header.Uname, header.Gname = 0, 0, "", ""
	header.AccessTime, header.ChangeTime = time.Time{}, time.Time{}
	header.Format = tar.FormatPAX
	if err := archive.WriteHeader(header); err != nil {
		return fmt.Errorf("write header of %s: %w", e.name, err)
	}
	if !e.info.Mode().IsRegular() {
		return nil
	}
	file, err := os.Open(filepath.Join(root, filepath.FromSlash(e.name)))
	if err != nil {
		return fmt.Errorf("open %s: %w", e.name, err)
	}
	defer file.Close()
	copied, err := io.Copy(archive, io.LimitReader(file, header.Size))
	if err != nil {
		return fmt.Errorf("read %s: %w", e.name, err)
	}
	if copied < header.Size {
		if _, err := io.CopyN(archive, zeros{}, header.Size-copied); err != nil {
			return fmt.Errorf("pad %s: %w", e.name, err)
		}
	}
	return nil
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) { clear(p); return len(p), nil }

// linkInside reports whether a symbolic link in the directory linkDir, with
// the target linkname, points to a place inside root. Both are real paths.
// It follows the links that are already made, so a target that goes through
// one is judged by where it ends. A target that does not exist yet is judged
// by its path, which linkEscapes has already checked.
func linkInside(root, linkDir, linkname string) bool {
	candidate := filepath.Join(linkDir, linkname)
	real, err := filepath.EvalSymlinks(candidate)
	if errors.Is(err, fs.ErrNotExist) {
		real, err = candidate, nil
	}
	if err != nil {
		return false
	}
	relative, err := filepath.Rel(root, real)
	return err == nil && !strings.HasPrefix(filepath.Clean(relative), "..")
}

// ErrUnsafeArchive is wrapped by the error of an archive that holds an entry
// that would be written outside the directory it is extracted into.
var ErrUnsafeArchive = errors.New("unsafe snapshot archive")

func unsafeEntry(name, why string) error {
	return fmt.Errorf("%w: entry %q %s", ErrUnsafeArchive, name, why)
}

// entryPath checks an entry's name and returns it cleaned. The root is ".".
func entryPath(name string) (string, error) {
	switch {
	case name == "":
		return "", unsafeEntry(name, "has no name")
	case strings.HasPrefix(name, "/"):
		return "", unsafeEntry(name, "is an absolute path")
	case strings.ContainsRune(name, 0):
		return "", unsafeEntry(name, "has a NUL in its name")
	}
	for element := range strings.SplitSeq(name, "/") {
		if element == ".." {
			return "", unsafeEntry(name, "leaves the workspace with '..'")
		}
	}
	clean := path.Clean(name)
	if !filepath.IsLocal(filepath.FromSlash(clean)) {
		return "", unsafeEntry(name, "is not a path inside the workspace")
	}
	return clean, nil
}

// extract reads a zstd-compressed tar from r into the existing directory
// dest, and reads the stream to its end, so that a stream that is cut or
// damaged is an error. It refuses entries that would be written outside
// dest, and entries other than regular files, directories and links.
func extract(r io.Reader, dest string) error {
	decompressor, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return err
	}
	defer decompressor.Close()
	archive := tar.NewReader(decompressor)

	type timed struct {
		path string
		mode fs.FileMode
		time time.Time
	}
	var directories []timed
	links := map[string]bool{}
	seen := false
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar: %w", err)
		}
		seen = true
		name, err := entryPath(header.Name)
		if err != nil {
			return err
		}
		// Nothing is written through a link: an entry whose parent is one
		// would land where the link points.
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if links[parent] {
				return unsafeEntry(header.Name, "is inside a symbolic link")
			}
		}
		target := filepath.Join(dest, filepath.FromSlash(name))
		// The name was checked above. This checks the path that is used.
		if base := filepath.Clean(dest); target != base && !strings.HasPrefix(target, base+string(filepath.Separator)) {
			return unsafeEntry(header.Name, "would be written outside the workspace")
		}
		mode := fs.FileMode(header.Mode).Perm()
		switch header.Typeflag {
		case tar.TypeDir:
			if name == "." {
				directories = append(directories, timed{target, mode, header.ModTime})
				continue
			}
			if err := os.MkdirAll(target, 0o700); err != nil {
				return err
			}
			directories = append(directories, timed{target, mode, header.ModTime})
		case tar.TypeReg:
			if name == "." {
				return unsafeEntry(header.Name, "is a file where the workspace is")
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return err
			}
			file, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(file, archive)
			closeErr := file.Close()
			if err := errors.Join(copyErr, closeErr); err != nil {
				return fmt.Errorf("write %s: %w", name, err)
			}
			// The mode is set after the write: the umask does not apply to
			// it, and a read-only file is still written.
			if err := os.Chmod(target, mode); err != nil {
				return err
			}
			if err := os.Chtimes(target, header.ModTime, header.ModTime); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if name == "." {
				return unsafeEntry(header.Name, "is a link where the workspace is")
			}
			if linkEscapes(name, header.Linkname) {
				return unsafeEntry(header.Name, "is a symbolic link to "+header.Linkname+", which may leave the workspace")
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return err
			}
			// The link is made in the directory that its parent really is,
			// which is inside dest.
			parent, err := filepath.EvalSymlinks(filepath.Dir(target))
			if err != nil {
				return err
			}
			root, err := filepath.EvalSymlinks(dest)
			if err != nil {
				return err
			}
			if parent != root && !strings.HasPrefix(parent, root+string(filepath.Separator)) {
				return unsafeEntry(header.Name, "would be made outside the workspace")
			}
			if !linkInside(root, parent, header.Linkname) {
				return unsafeEntry(header.Name, "is a symbolic link to "+header.Linkname+", which leaves the workspace")
			}
			if err := os.Symlink(header.Linkname, filepath.Join(parent, filepath.Base(target))); err != nil {
				return err
			}
			links[name] = true
		default:
			return unsafeEntry(header.Name, fmt.Sprintf("has the type %q, and only files, directories and symbolic links are restored", header.Typeflag))
		}
	}
	if !seen {
		return errors.New("the snapshot holds no entries")
	}
	// The tar ended. The zstd stream must too, with its checksum read.
	if _, err := io.Copy(io.Discard, decompressor); err != nil {
		return fmt.Errorf("read zstd: %w", err)
	}
	// A directory's mode and time are set last, deepest first: a read-only
	// directory could not be filled, and filling one changes its time.
	sort.Slice(directories, func(i, j int) bool { return directories[i].path > directories[j].path })
	for _, d := range directories {
		if err := os.Chmod(d.path, d.mode); err != nil {
			return err
		}
		if err := os.Chtimes(d.path, d.time, d.time); err != nil {
			return err
		}
	}
	return nil
}
