package artifact

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// The S3 tests run against a real SeaweedFS, which is what the cluster runs.
// Point them at a service with ARTIFACT_TEST_S3_ENDPOINT, ARTIFACT_TEST_S3_ACCESS_KEY and
// ARTIFACT_TEST_S3_SECRET_KEY (the bucket is made), or at a weed binary with
// ARTIFACT_TEST_WEED, which the test starts with a key of its own. With neither
// they skip.

const (
	testAccessKey = "artifact-test-access"
	testSecretKey = "artifact-test-secret"
)

// testS3 returns the configuration of a fresh bucket.
func testS3(t *testing.T) S3Config {
	t.Helper()
	config := S3Config{
		Endpoint:  os.Getenv("ARTIFACT_TEST_S3_ENDPOINT"),
		AccessKey: os.Getenv("ARTIFACT_TEST_S3_ACCESS_KEY"),
		SecretKey: os.Getenv("ARTIFACT_TEST_S3_SECRET_KEY"),
	}
	if config.Endpoint == "" {
		weed := os.Getenv("ARTIFACT_TEST_WEED")
		if weed == "" {
			t.Skip("set ARTIFACT_TEST_WEED or ARTIFACT_TEST_S3_ENDPOINT to test against SeaweedFS")
		}
		config.Endpoint = startWeed(t, weed)
		config.AccessKey, config.SecretKey = testAccessKey, testSecretKey
	}
	config.Bucket = fmt.Sprintf("p7-artifacts-test-%d", time.Now().UnixNano())
	client, err := minio.New(config.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""), BucketLookup: minio.BucketLookupPath})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := client.MakeBucket(ctx, config.Bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatalf("make the bucket: %v", err)
	}
	return config
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

var (
	weedOnce    sync.Once
	weedAddress string
	weedProcess *exec.Cmd
	weedDir     string
)

// TestMain stops the SeaweedFS that the tests started, once for the package.
func TestMain(m *testing.M) {
	code := m.Run()
	if weedProcess != nil {
		_ = weedProcess.Process.Kill()
		_ = weedProcess.Wait()
		os.RemoveAll(weedDir)
	}
	os.Exit(code)
}

// startWeed runs SeaweedFS with one S3 identity, once, and returns the S3
// address.
func startWeed(t *testing.T, weed string) string {
	t.Helper()
	weedOnce.Do(func() {
		dir, err := os.MkdirTemp("", "weed-")
		if err != nil {
			t.Fatal(err)
		}
		weedDir = dir
		if err := os.Mkdir(filepath.Join(dir, "data"), 0o755); err != nil {
			t.Fatal(err)
		}
		identities := fmt.Sprintf(`{"identities":[{"name":"artifacts","credentials":[{"accessKey":%q,"secretKey":%q}],"actions":["Admin","Read","Write","List","Tagging"]}]}`, testAccessKey, testSecretKey)
		if err := os.WriteFile(filepath.Join(dir, "s3.json"), []byte(identities), 0o600); err != nil {
			t.Fatal(err)
		}
		master, volume, filer, s3 := freePort(t), freePort(t), freePort(t), freePort(t)
		command := exec.Command(weed, "server", "-dir="+filepath.Join(dir, "data"), "-ip=127.0.0.1",
			fmt.Sprintf("-master.port=%d", master), fmt.Sprintf("-volume.port=%d", volume), fmt.Sprintf("-filer.port=%d", filer),
			"-s3", fmt.Sprintf("-s3.port=%d", s3), "-s3.config="+filepath.Join(dir, "s3.json"), "-master.volumeSizeLimitMB=32", "-volume.max=100")
		logFile, err := os.Create(filepath.Join(dir, "weed.log"))
		if err != nil {
			t.Fatal(err)
		}
		command.Stdout, command.Stderr = logFile, logFile
		if err := command.Start(); err != nil {
			t.Fatalf("start weed: %v", err)
		}
		weedProcess = command
		weedAddress = fmt.Sprintf("127.0.0.1:%d", s3)
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			connection, err := net.DialTimeout("tcp", weedAddress, time.Second)
			if err == nil {
				connection.Close()
				// The S3 gateway listens before the filer behind it is ready.
				time.Sleep(3 * time.Second)
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
		weedAddress = ""
	})
	if weedAddress == "" {
		log, _ := os.ReadFile(filepath.Join(weedDir, "weed.log"))
		t.Fatalf("weed did not start:\n%s", tail(string(log), 4000))
	}
	return weedAddress
}

func tail(s string, n int) string {
	if len(s) > n {
		return "..." + s[len(s)-n:]
	}
	return strings.TrimSpace(s)
}
