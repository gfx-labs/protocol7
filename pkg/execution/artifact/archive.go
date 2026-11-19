package artifact

import (
	"archive/zip"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// dirComment marks a zip that holds a saved directory, in the zip's own
// comment. A directory is stored as a zip whose bytes depend only on the
// files, so saving the same directory twice is the same digest. The mark is
// in the bytes, so a reference alone says whether to unpack: a zip that a
// person saved as a file has no mark and stays a file.
const dirComment = "p7-artifact-dir"

// maxFiles bounds the files of a directory, saved or unpacked.
const maxFiles = 10000

// zipDir writes the files under dir as a zip, in name order and with no
// times. It leaves out .git and anything that is not a regular file, so a
// link cannot lead out of the tree. Contents beyond limit bytes are an error.
func zipDir(w io.Writer, dir fs.FS, limit int64) error {
	archive := zip.NewWriter(w)
	if err := archive.SetComment(dirComment); err != nil {
		return err
	}
	var total int64
	var files int
	err := fs.WalkDir(dir, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return fs.SkipDir
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if files++; files > maxFiles {
			return fmt.Errorf("the directory has more than %d files: %w", maxFiles, ErrTooLarge)
		}
		if total += info.Size(); total > limit {
			return fmt.Errorf("the directory is larger than %d MiB: %w", limit>>20, ErrTooLarge)
		}
		header := &zip.FileHeader{Name: name, Method: zip.Deflate}
		if info.Mode()&0o111 != 0 {
			header.SetMode(0o755)
		} else {
			header.SetMode(0o644)
		}
		out, err := archive.CreateHeader(header)
		if err != nil {
			return err
		}
		file, err := dir.Open(name)
		if err != nil {
			return err
		}
		defer file.Close()
		// The file may have grown since it was listed.
		_, err = io.Copy(out, io.LimitReader(file, info.Size()))
		return err
	})
	if err != nil {
		return err
	}
	return archive.Close()
}

// openDirArchive returns the zip of a saved directory, or false for bytes that
// are not one.
func openDirArchive(file *os.File) (*zip.Reader, bool) {
	info, err := file.Stat()
	if err != nil {
		return nil, false
	}
	archive, err := zip.NewReader(file, info.Size())
	if err != nil || archive.Comment != dirComment {
		return nil, false
	}
	return archive, true
}

// unzip writes the files of a saved directory under dest in root. It stops
// at a name that leaves dest, at a link, and at more bytes than limit.
func unzip(root *os.Root, dest string, archive *zip.Reader, limit int64) (files int, size int64, err error) {
	if len(archive.File) > maxFiles {
		return 0, 0, fmt.Errorf("the archive has more than %d files: %w", maxFiles, ErrTooLarge)
	}
	var declared uint64
	for _, file := range archive.File {
		declared += file.UncompressedSize64
	}
	if declared > uint64(limit) {
		return 0, 0, fmt.Errorf("the archive unpacks to more than %d MiB: %w", limit>>20, ErrTooLarge)
	}
	if err := root.MkdirAll(dest, 0o755); err != nil {
		return 0, 0, err
	}
	for _, file := range archive.File {
		name := file.Name
		if !filepath.IsLocal(filepath.FromSlash(name)) || strings.Contains(name, `\`) {
			return files, size, invalidf("the archive holds %q, which is not inside the directory", name)
		}
		target := path.Join(dest, name)
		mode := file.Mode()
		switch {
		case file.FileInfo().IsDir():
			if err := root.MkdirAll(target, 0o755); err != nil {
				return files, size, err
			}
			continue
		case !mode.IsRegular():
			return files, size, invalidf("the archive holds %q, which is not a regular file", name)
		}
		if err := root.MkdirAll(path.Dir(target), 0o755); err != nil {
			return files, size, err
		}
		written, err := writeFile(root, target, openZipEntry(file), mode&0o111 != 0, limit-size)
		size += written
		if err != nil {
			return files, size, err
		}
		files++
	}
	return files, size, nil
}

func openZipEntry(file *zip.File) func() (io.ReadCloser, error) { return file.Open }

// writeFile writes what open returns to name in root, at most limit bytes.
func writeFile(root *os.Root, name string, open func() (io.ReadCloser, error), executable bool, limit int64) (int64, error) {
	reader, err := open()
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	return copyFile(root, name, reader, executable, limit)
}

func copyFile(root *os.Root, name string, reader io.Reader, executable bool, limit int64) (int64, error) {
	mode := os.FileMode(0o644)
	if executable {
		mode = 0o755
	}
	// A symbolic link at name that leads out of the workspace is refused by
	// the root, and one that stays in it is followed.
	out, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(out, io.LimitReader(reader, limit+1))
	if closeErr := out.Close(); err == nil {
		err = closeErr
	}
	if err == nil && n > limit {
		err = fmt.Errorf("more than the size limit: %w", ErrTooLarge)
	}
	return n, err
}
