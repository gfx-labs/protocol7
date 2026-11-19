package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ErrNotFound is returned for a key, digest or name that is not there.
var ErrNotFound = errors.New("not found")

// Info describes one object.
type Info struct {
	Key      string
	Size     int64
	Modified time.Time
}

// Blobs is the object store under the artifacts: a flat key space, as in an
// S3 bucket. The S3 store is the real one, the directory store is for tests
// and for a workstation without a bucket.
type Blobs interface {
	// Put stores size bytes from r under key, replacing what is there.
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	// Get opens the object. It returns ErrNotFound for a missing key.
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// Stat returns ErrNotFound for a missing key.
	Stat(ctx context.Context, key string) (Info, error)
	// List returns every object whose key starts with prefix, by key.
	List(ctx context.Context, prefix string) ([]Info, error)
	// Delete removes the object. A missing key is not an error.
	Delete(ctx context.Context, key string) error
}

// S3Config says where the bucket is and how to sign in to it.
type S3Config struct {
	// Endpoint is host:port of the S3 service, such as
	// seaweedfs-s3.seaweedfs:8333.
	Endpoint string
	Bucket   string
	// AccessKey and SecretKey are the key limited to the bucket.
	AccessKey, SecretKey string
	// TLS dials the endpoint over HTTPS.
	TLS bool
	// Region is the signing region. Empty is us-east-1, which SeaweedFS
	// accepts.
	Region string
}

type s3Blobs struct {
	client *minio.Client
	bucket string
}

// NewS3 returns the store of a bucket. It does not contact the service: the
// first call does.
func NewS3(config S3Config) (Blobs, error) {
	if config.Endpoint == "" || config.Bucket == "" {
		return nil, errors.New("artifacts: an S3 endpoint and a bucket are required")
	}
	if config.AccessKey == "" || config.SecretKey == "" {
		return nil, errors.New("artifacts: the S3 access key and secret key are required")
	}
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""),
		Secure: config.TLS,
		Region: config.Region,
		// SeaweedFS serves buckets by path.
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return nil, fmt.Errorf("artifacts: configure S3: %w", err)
	}
	return &s3Blobs{client: client, bucket: config.Bucket}, nil
}

func (s *s3Blobs) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, r, size, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

func (s *s3Blobs) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	object, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, s.wrap("get", key, err)
	}
	// GetObject is lazy: the first read tells whether the key exists.
	if _, err := object.Stat(); err != nil {
		object.Close()
		return nil, s.wrap("get", key, err)
	}
	return object, nil
}

func (s *s3Blobs) Stat(ctx context.Context, key string) (Info, error) {
	stat, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return Info{}, s.wrap("stat", key, err)
	}
	return Info{Key: key, Size: stat.Size, Modified: stat.LastModified}, nil
}

func (s *s3Blobs) List(ctx context.Context, prefix string) ([]Info, error) {
	var infos []Info
	for object := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if object.Err != nil {
			return nil, fmt.Errorf("list %s: %w", prefix, object.Err)
		}
		infos = append(infos, Info{Key: object.Key, Size: object.Size, Modified: object.LastModified})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Key < infos[j].Key })
	return infos, nil
}

func (s *s3Blobs) Delete(ctx context.Context, key string) error {
	if err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

func (s *s3Blobs) wrap(verb, key string, err error) error {
	switch minio.ToErrorResponse(err).Code {
	case "NoSuchKey", "NoSuchBucket":
		return fmt.Errorf("%s %s: %w", verb, key, ErrNotFound)
	}
	return fmt.Errorf("%s %s: %w", verb, key, err)
}

// dirBlobs keeps each object as a file under a directory. A key is a path,
// so a name that is the directory of another cannot be stored: the S3 store
// has no such limit.
type dirBlobs struct{ root string }

// NewDir returns a store in a directory, which is made if it is not there.
func NewDir(root string) (Blobs, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("artifacts: %w", err)
	}
	return dirBlobs{root: root}, nil
}

func (d dirBlobs) path(key string) (string, error) {
	// Keys are made of validated parts, but a store does not trust that.
	if key == "" || !filepath.IsLocal(filepath.FromSlash(key)) {
		return "", fmt.Errorf("the key %q is not local", key)
	}
	return filepath.Join(d.root, filepath.FromSlash(key)), nil
}

func (d dirBlobs) Put(_ context.Context, key string, r io.Reader, size int64) error {
	path, err := d.path(key)
	if err != nil {
		return err
	}
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return fmt.Errorf("put %s: the directory store cannot keep a name that is the directory of another", key)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("put %s: the directory store cannot keep a name that is under another name: %w", key, err)
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".put-*")
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	defer os.Remove(temp.Name())
	n, err := io.Copy(temp, r)
	if err == nil && n != size {
		err = fmt.Errorf("got %d bytes, want %d", n, size)
	}
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Chmod(temp.Name(), 0o644)
	}
	if err == nil {
		err = os.Rename(temp.Name(), path)
	}
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

func (d dirBlobs) Get(_ context.Context, key string) (io.ReadCloser, error) {
	path, err := d.path(key)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, wrapFile("get", key, err)
	}
	// A key that is the directory of others is not an object.
	if info, err := file.Stat(); err != nil || info.IsDir() {
		file.Close()
		return nil, fmt.Errorf("get %s: %w", key, ErrNotFound)
	}
	return file, nil
}

func (d dirBlobs) Stat(_ context.Context, key string) (Info, error) {
	path, err := d.path(key)
	if err != nil {
		return Info{}, err
	}
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		if err == nil {
			err = fs.ErrNotExist
		}
		return Info{}, wrapFile("stat", key, err)
	}
	return Info{Key: key, Size: info.Size(), Modified: info.ModTime()}, nil
}

func (d dirBlobs) List(_ context.Context, prefix string) ([]Info, error) {
	var infos []Info
	err := filepath.WalkDir(d.root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".put-") {
			return nil
		}
		relative, err := filepath.Rel(d.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(relative)
		if !strings.HasPrefix(key, prefix) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		infos = append(infos, Info{Key: key, Size: info.Size(), Modified: info.ModTime()})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", prefix, err)
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Key < infos[j].Key })
	return infos, nil
}

func (d dirBlobs) Delete(_ context.Context, key string) error {
	path, err := d.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

func wrapFile(verb, key string, err error) error {
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		return fmt.Errorf("%s %s: %w", verb, key, ErrNotFound)
	}
	return fmt.Errorf("%s %s: %w", verb, key, err)
}
