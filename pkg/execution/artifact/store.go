package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	// DefaultMaxSize bounds one artifact, and the size of a zip's contents
	// when it is unpacked.
	DefaultMaxSize = 256 << 20
	// DefaultRetention is how long an artifact is kept unless pinned.
	DefaultRetention = 90 * 24 * time.Hour
	// maxVersions bounds the digests a name remembers.
	maxVersions = 100
	// maxIndexSize bounds a name index when it is read.
	maxIndexSize = 1 << 20
)

// Version is one digest a name pointed at.
type Version struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
	// Saved is when the name was last pointed at this digest.
	Saved time.Time `json:"saved"`
	// By says who saved it: a chat's session or a person's login.
	By string `json:"by,omitempty"`
	// Pinned keeps this digest beyond the retention. The newest digest of a
	// name is kept whether or not it is pinned.
	Pinned bool `json:"pinned,omitempty"`
}

// Ref is the reference to the version, displayed with the name.
func (v Version) Ref(name string) Ref { return Ref{Digest: v.Digest, Name: name} }

// index is the object at <project>/<name>: the digests the name has pointed
// at, oldest first. The last one is the name's current digest.
type index struct {
	Versions []Version `json:"versions"`
}

// Entry is a name as a list shows it.
type Entry struct {
	Project string `json:"project"`
	Name    string `json:"name"`
	Version
	// Versions is how many digests the name remembers, this one included.
	Versions int `json:"versions"`
	// Kept says why the digest is not subject to retention: "newest" or
	// "pinned".
	Kept string `json:"kept"`
}

// Store is the artifacts of a bucket.
type Store struct {
	Blobs Blobs
	// MaxSize is the largest artifact. Zero is DefaultMaxSize.
	MaxSize int64
	// Retention is how long an artifact that is neither the newest of a name
	// nor pinned is kept. Zero is DefaultRetention.
	Retention time.Duration
	// TempDir holds the file that a save or a read is staged in. Empty is the
	// system's.
	TempDir string
	// Now is the clock. Nil is time.Now.
	Now func() time.Time
}

func (s *Store) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func (s *Store) maxSize() int64 {
	if s.MaxSize > 0 {
		return s.MaxSize
	}
	return DefaultMaxSize
}

func (s *Store) retention() time.Duration {
	if s.Retention > 0 {
		return s.Retention
	}
	return DefaultRetention
}

// ErrTooLarge is returned for bytes beyond the size limit.
var ErrTooLarge = errors.New("too large")

// Save stores the bytes of src under their digest and makes name in project
// point at it. The bytes are staged in a temporary file first, because the
// key is the digest. The name keeps what it pointed at before.
func (s *Store) Save(ctx context.Context, project, name, by string, src io.Reader) (Version, error) {
	if err := ValidProject(project); err != nil {
		return Version{}, err
	}
	if err := ValidName(name); err != nil {
		return Version{}, err
	}
	staged, err := os.CreateTemp(s.TempDir, "artifact-*")
	if err != nil {
		return Version{}, fmt.Errorf("stage the artifact: %w", err)
	}
	defer os.Remove(staged.Name())
	defer staged.Close()
	hash := sha256.New()
	limit := s.maxSize()
	size, err := io.Copy(io.MultiWriter(staged, hash), io.LimitReader(src, limit+1))
	if err != nil {
		return Version{}, fmt.Errorf("read the artifact: %w", err)
	}
	if size > limit {
		return Version{}, fmt.Errorf("the artifact is larger than %d MiB: %w", limit>>20, ErrTooLarge)
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	// Bytes that are already there are not sent again, unless they are old
	// enough that retention may be about to remove them.
	info, err := s.Blobs.Stat(ctx, blobKey(digest))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Version{}, err
	}
	if err != nil || s.now().Sub(info.Modified) > s.retention()/2 {
		if _, err := staged.Seek(0, io.SeekStart); err != nil {
			return Version{}, err
		}
		if err := s.Blobs.Put(ctx, blobKey(digest), staged, size); err != nil {
			return Version{}, err
		}
	}
	version := Version{Digest: digest, Size: size, Saved: s.now(), By: by}
	if err := s.update(ctx, project, name, func(current index) index {
		// Saving again moves the digest to the front of the line, keeping its pin.
		kept := current.Versions[:0:0]
		for _, earlier := range current.Versions {
			if earlier.Digest == digest {
				version.Pinned = version.Pinned || earlier.Pinned
				continue
			}
			kept = append(kept, earlier)
		}
		kept = append(kept, version)
		if extra := len(kept) - maxVersions; extra > 0 {
			kept = dropOldest(kept, extra)
		}
		return index{Versions: kept}
	}); err != nil {
		return Version{}, err
	}
	return version, nil
}

// dropOldest removes up to n of the oldest versions that are not pinned. The
// last version is never removed.
func dropOldest(versions []Version, n int) []Version {
	kept := versions[:0:0]
	for i, version := range versions {
		if n > 0 && !version.Pinned && i < len(versions)-1 {
			n--
			continue
		}
		kept = append(kept, version)
	}
	return kept
}

// Open returns a reader of the bytes of a digest. The caller closes it and
// should check that the bytes match the digest: see Fetch.
func (s *Store) Open(ctx context.Context, digest string) (io.ReadCloser, error) {
	if !ValidDigest(digest) {
		return nil, invalidf("%q is not a digest", digest)
	}
	reader, err := s.Blobs.Get(ctx, blobKey(digest))
	if err != nil {
		return nil, fmt.Errorf("sha256:%s: %w", digest, err)
	}
	return reader, nil
}

// Fetch copies the bytes of a digest into a temporary file, checks them
// against the digest and returns the file, positioned at its start. The caller
// closes it and removes it with the returned function.
func (s *Store) Fetch(ctx context.Context, digest string) (*os.File, func(), error) {
	reader, err := s.Open(ctx, digest)
	if err != nil {
		return nil, nil, err
	}
	defer reader.Close()
	staged, err := os.CreateTemp(s.TempDir, "artifact-*")
	if err != nil {
		return nil, nil, fmt.Errorf("stage the artifact: %w", err)
	}
	cleanup := func() {
		staged.Close()
		os.Remove(staged.Name())
	}
	hash := sha256.New()
	limit := s.maxSize()
	size, err := io.Copy(io.MultiWriter(staged, hash), io.LimitReader(reader, limit+1))
	if err == nil && size > limit {
		err = fmt.Errorf("the artifact is larger than %d MiB: %w", limit>>20, ErrTooLarge)
	}
	if err == nil && hex.EncodeToString(hash.Sum(nil)) != digest {
		err = fmt.Errorf("the stored bytes of sha256:%s do not match their digest", digest)
	}
	if err == nil {
		_, err = staged.Seek(0, io.SeekStart)
	}
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return staged, cleanup, nil
}

// Lookup returns the version a name points at now.
func (s *Store) Lookup(ctx context.Context, project, name string) (Version, error) {
	current, err := s.read(ctx, project, name)
	if err != nil {
		return Version{}, err
	}
	return current.Versions[len(current.Versions)-1], nil
}

// Versions returns every digest a name remembers, newest first.
func (s *Store) Versions(ctx context.Context, project, name string) ([]Version, error) {
	current, err := s.read(ctx, project, name)
	if err != nil {
		return nil, err
	}
	versions := make([]Version, len(current.Versions))
	for i, version := range current.Versions {
		versions[len(versions)-1-i] = version
	}
	return versions, nil
}

// Stat says how big an artifact is, and whether it is still there.
func (s *Store) Stat(ctx context.Context, digest string) (Info, error) {
	if !ValidDigest(digest) {
		return Info{}, invalidf("%q is not a digest", digest)
	}
	return s.Blobs.Stat(ctx, blobKey(digest))
}

// Pin keeps one digest of a name beyond the retention, or lets it go again.
// The newest digest of a name is kept without a pin.
func (s *Store) Pin(ctx context.Context, project, name, digest string, pinned bool) error {
	found := false
	if err := s.update(ctx, project, name, func(current index) index {
		versions := append([]Version(nil), current.Versions...)
		for i := range versions {
			if versions[i].Digest == digest {
				versions[i].Pinned = pinned
				found = true
			}
		}
		return index{Versions: versions}
	}); err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%s/%s has no digest sha256:%s: %w", project, name, digest, ErrNotFound)
	}
	return nil
}

// MaxList bounds the names a list returns.
const MaxList = 1000

// List returns the names of a project that start with prefix, with the digest
// each points at now.
func (s *Store) List(ctx context.Context, project, prefix string) ([]Entry, error) {
	if err := ValidProject(project); err != nil {
		return nil, err
	}
	infos, err := s.Blobs.List(ctx, project+"/"+prefix)
	if err != nil {
		return nil, err
	}
	if len(infos) > MaxList {
		infos = infos[:MaxList]
	}
	entries := make([]Entry, 0, len(infos))
	for _, info := range infos {
		name := strings.TrimPrefix(info.Key, project+"/")
		current, err := s.read(ctx, project, name)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		newest := current.Versions[len(current.Versions)-1]
		kept := "newest"
		entries = append(entries, Entry{Project: project, Name: name, Version: newest, Versions: len(current.Versions), Kept: kept})
		if newest.Pinned {
			entries[len(entries)-1].Kept = "pinned"
		}
	}
	return entries, nil
}

func (s *Store) read(ctx context.Context, project, name string) (index, error) {
	if err := ValidProject(project); err != nil {
		return index{}, err
	}
	if err := ValidName(name); err != nil {
		return index{}, err
	}
	reader, err := s.Blobs.Get(ctx, indexKey(project, name))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return index{}, fmt.Errorf("no artifact named %s/%s: %w", project, name, ErrNotFound)
		}
		return index{}, err
	}
	defer reader.Close()
	return decodeIndex(reader, project, name)
}

func decodeIndex(reader io.Reader, project, name string) (index, error) {
	var current index
	if err := json.NewDecoder(io.LimitReader(reader, maxIndexSize)).Decode(&current); err != nil {
		return index{}, fmt.Errorf("the index of %s/%s is damaged: %w", project, name, err)
	}
	if len(current.Versions) == 0 {
		return index{}, fmt.Errorf("the index of %s/%s is empty: %w", project, name, ErrNotFound)
	}
	return current, nil
}

// update reads a name's index, changes it and writes it back. Two writers of
// the same name at the same moment may lose one of the changes: S3 has no
// compare-and-swap that every service supports, and a name has one author at a
// time in practice.
func (s *Store) update(ctx context.Context, project, name string, change func(index) index) error {
	current, err := s.read(ctx, project, name)
	if errors.Is(err, ErrNotFound) {
		current, err = index{}, nil
	}
	if err != nil {
		return err
	}
	return s.write(ctx, project, name, change(current))
}

func (s *Store) write(ctx context.Context, project, name string, value index) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return s.Blobs.Put(ctx, indexKey(project, name), strings.NewReader(string(data)), int64(len(data)))
}

// SweepResult counts what a sweep removed.
type SweepResult struct {
	// Versions and Blobs count what was removed.
	Versions, Blobs int
	// Bytes is the size of the removed blobs.
	Bytes int64
}

// prune is the versions that retention keeps: the newest, the pinned, and
// those saved since cutoff.
func prune(versions []Version, cutoff time.Time) []Version {
	kept := versions[:0:0]
	for i, version := range versions {
		if i < len(versions)-1 && !version.Pinned && version.Saved.Before(cutoff) {
			continue
		}
		kept = append(kept, version)
	}
	return kept
}

// Sweep applies the retention. A version of a name goes when it is older than
// the retention and is neither the newest of its name nor pinned. A digest goes
// when no name remembers it and it is older than the retention. It may run at
// any time: it reads an index again before it writes it, and checks a blob's age
// again before it deletes it. A save of the same name in the instant between that
// read and write can still be lost, as with any two writers of a name.
func (s *Store) Sweep(ctx context.Context) (SweepResult, error) {
	var result SweepResult
	cutoff := s.now().Add(-s.retention())
	all, err := s.Blobs.List(ctx, "")
	if err != nil {
		return result, err
	}
	referenced := map[string]bool{}
	var blobs []Info
	for _, info := range all {
		if digest, ok := strings.CutPrefix(info.Key, BlobPrefix); ok {
			if ValidDigest(digest) {
				blobs = append(blobs, info)
			}
			continue
		}
		project, name, ok := strings.Cut(info.Key, "/")
		if !ok || ValidProject(project) != nil || ValidName(name) != nil {
			continue
		}
		current, err := s.read(ctx, project, name)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return result, err
		}
		kept := prune(current.Versions, cutoff)
		if len(kept) != len(current.Versions) {
			// Read the index again right before the write, so that a save
			// that came in since the first read is not dropped.
			if current, err = s.read(ctx, project, name); err != nil {
				return result, err
			}
			kept = prune(current.Versions, cutoff)
			if len(kept) != len(current.Versions) {
				if err := s.write(ctx, project, name, index{Versions: kept}); err != nil {
					return result, err
				}
				result.Versions += len(current.Versions) - len(kept)
			}
		}
		for _, version := range kept {
			referenced[version.Digest] = true
		}
	}
	sort.Slice(blobs, func(i, j int) bool { return blobs[i].Key < blobs[j].Key })
	for _, blob := range blobs {
		digest := strings.TrimPrefix(blob.Key, BlobPrefix)
		if referenced[digest] || !blob.Modified.Before(cutoff) {
			continue
		}
		// The listing is from the start of the sweep. A blob that was uploaded
		// again since then is new, and stays.
		if current, err := s.Blobs.Stat(ctx, blob.Key); err != nil || !current.Modified.Before(cutoff) {
			if err != nil && !errors.Is(err, ErrNotFound) {
				return result, err
			}
			continue
		}
		if err := s.Blobs.Delete(ctx, blob.Key); err != nil {
			return result, err
		}
		result.Blobs++
		result.Bytes += blob.Size
	}
	return result, nil
}
