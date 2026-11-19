package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.uber.org/fx"

	"github.com/gfx-labs/protocol7/pkg/execution/artifact"
	"github.com/gfx-labs/protocol7/pkg/execution/design"
	"github.com/gfx-labs/protocol7/pkg/execution/kubeop"
	"github.com/gfx-labs/protocol7/pkg/p7"
	"github.com/gfx-labs/protocol7/pkg/p7/p7fx"
)

// artifactFlags say where the artifact bucket is. The processes that hold the
// bucket's key (the one beside the core worker, and the web server) and the
// shell commands all take them.
type artifactFlags struct {
	Endpoint      string `name:"artifact-s3-endpoint" env:"SELFDEV_ARTIFACT_S3_ENDPOINT" help:"host:port of the S3 service that holds the artifact bucket, such as seaweedfs-s3.seaweedfs:8333. With --artifact-dir unset and this empty, artifacts are off."`
	Bucket        string `name:"artifact-bucket" default:"p7-artifacts" env:"SELFDEV_ARTIFACT_BUCKET" help:"The artifact bucket. Nothing else is written to it."`
	AccessKeyFile string `name:"artifact-access-key-file" env:"SELFDEV_ARTIFACT_ACCESS_KEY_FILE" help:"File that holds the access key of a key limited to the artifact bucket."`
	SecretKeyFile string `name:"artifact-secret-key-file" env:"SELFDEV_ARTIFACT_SECRET_KEY_FILE" help:"File that holds that key's secret."`
	TLS           bool   `name:"artifact-s3-tls" env:"SELFDEV_ARTIFACT_S3_TLS" help:"Dial the S3 service over HTTPS."`
	Dir           string `name:"artifact-dir" env:"SELFDEV_ARTIFACT_DIR" help:"Keep artifacts in this directory instead of a bucket, for a workstation. Overrides --artifact-s3-endpoint."`
	MaxSize       int64  `name:"artifact-max-size" default:"268435456" env:"SELFDEV_ARTIFACT_MAX_SIZE" help:"Largest artifact in bytes, and largest unpacked directory."`
	Retention     string `name:"artifact-retention" default:"2160h" env:"SELFDEV_ARTIFACT_RETENTION" help:"How long an artifact that is not the newest of its name and not pinned is kept. Default 90 days."`
	TempDir       string `name:"artifact-temp-dir" env:"SELFDEV_ARTIFACT_TEMP_DIR" help:"Where uploads and downloads are staged. Default: the system's."`
}

// enabled says whether a bucket or a directory is named. A bucket whose key
// files are not there yet is off, with a note: the deployment mounts the key
// as an optional secret, so that a pod starts before the key is provisioned
// and its other work is not held up by it.
func (f artifactFlags) enabled() bool {
	if f.Dir != "" {
		return true
	}
	if f.Endpoint == "" {
		return false
	}
	for _, key := range []string{f.AccessKeyFile, f.SecretKeyFile} {
		if _, err := os.Stat(key); key != "" && errors.Is(err, fs.ErrNotExist) {
			fmt.Fprintf(os.Stderr, "files: artifacts are off: the bucket's key file %s is not there\n", key)
			return false
		}
	}
	return true
}

func readKey(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// store opens the artifact store the flags name. It does not contact the
// bucket.
func (f artifactFlags) store() (*artifact.Store, error) {
	retention, err := time.ParseDuration(f.Retention)
	if err != nil || retention <= 0 {
		return nil, fmt.Errorf("--artifact-retention %q: want a duration such as 2160h", f.Retention)
	}
	var blobs artifact.Blobs
	switch {
	case f.Dir != "":
		blobs, err = artifact.NewDir(f.Dir)
	case f.Endpoint != "":
		if f.AccessKeyFile == "" || f.SecretKeyFile == "" {
			return nil, errors.New("--artifact-access-key-file and --artifact-secret-key-file are required with --artifact-s3-endpoint")
		}
		var access, secret string
		if access, err = readKey(f.AccessKeyFile); err != nil {
			return nil, fmt.Errorf("--artifact-access-key-file: %w", err)
		}
		if secret, err = readKey(f.SecretKeyFile); err != nil {
			return nil, fmt.Errorf("--artifact-secret-key-file: %w", err)
		}
		blobs, err = artifact.NewS3(artifact.S3Config{Endpoint: f.Endpoint, Bucket: f.Bucket, AccessKey: access, SecretKey: secret, TLS: f.TLS})
	default:
		return nil, errors.New("artifacts are not set up: give --artifact-s3-endpoint or --artifact-dir")
	}
	if err != nil {
		return nil, err
	}
	return &artifact.Store{Blobs: blobs, MaxSize: f.MaxSize, Retention: retention, TempDir: f.TempDir}, nil
}

// filesCommand serves the sessions' workspaces for design boards and renders
// their screenshots. It runs beside the core worker, on the same machine or
// in the same pod. With artifacts or the operator credential on, it also
// serves those activities on the queue selfdev-artifacts-<host>.
type filesCommand struct {
	Listen   string `name:"listen" default:"127.0.0.1:8788" env:"SELFDEV_FILES_LISTEN" help:"Address the file server listens on. It has no authentication: whatever reaches it reads every session's workspace."`
	StateDir string `name:"state-dir" required:"" env:"P7_STATE_DIR" help:"The core worker's state directory, as given to p7-worker --state-dir. Read for design boards, and written by artifact_get into a session's workspace."`
	// Artifacts are served from here too: the process beside the core worker
	// reads and writes a session's workspace, and holds the bucket's key.
	Temporal  temporalFlags `embed:""`
	Host      string        `name:"host" env:"P7_HOST" help:"Machine this process runs beside, whose core worker's sessions it serves artifacts for. It polls the queue selfdev-artifacts-<host>. Default: the hostname."`
	Artifacts artifactFlags `embed:""`
	// OperatorCredentials makes this process place the operator credential
	// (pkg/execution/kubeop) in a session's workspace, on the same queue.
	OperatorCredentials bool   `name:"operator-credentials" env:"SELFDEV_OPERATOR_CREDENTIALS" help:"Poll the queue selfdev-artifacts-<host> to place the operator credential that the app's worker issued in a session's workspace, even with artifacts off."`
	Chromium            string `name:"chromium" default:"chromium-headless-shell" env:"SELFDEV_CHROMIUM" help:"Headless Chromium command for screenshots. Empty turns screenshots off."`
}

func (c *filesCommand) options() (fx.Option, error) {
	options := fx.Options(
		fx.StopTimeout(stopTimeout),
		fx.Supply(design.FilesConfig{Listen: c.Listen, StateDir: c.StateDir, Chromium: c.Chromium}),
		filesModule,
	)
	artifacts := c.Artifacts.enabled()
	if !artifacts && !c.OperatorCredentials {
		return options, nil
	}
	host, err := hostName(c.Host)
	if err != nil {
		return nil, err
	}
	temporal := c.Temporal.config()
	config := p7.WorkerConfig{TaskQueue: artifact.Queue(host), TemporalAddress: temporal.Address, TemporalNamespace: temporal.Namespace, TemporalSPIFFE: temporal.SPIFFE}
	// One worker serves every activity of the queue.
	options = fx.Options(options, fx.Supply(config), p7fx.Worker)
	if c.OperatorCredentials {
		options = fx.Options(options, fx.Supply(kubeop.Writer{StateDir: c.StateDir}), operatorWriterModule)
	}
	if artifacts {
		store, err := c.Artifacts.store()
		if err != nil {
			return nil, err
		}
		options = fx.Options(options, fx.Supply(artifact.Activities{Store: store, StateDir: c.StateDir}), artifactWorkerModule)
	}
	return options, nil
}

func (c *filesCommand) Run(ctx context.Context, logger *slog.Logger) error {
	options, err := c.options()
	if err != nil {
		return err
	}
	return p7fx.Run(ctx, logger, options)
}

// filesModule is the session file server. It needs a design.FilesConfig.
var filesModule = fx.Module("files",
	fx.Provide(func(lifecycle fx.Lifecycle, shutdowner fx.Shutdowner, config design.FilesConfig, logger *slog.Logger) (*design.Files, error) {
		files, err := design.NewFiles(config, logger)
		if err != nil {
			return nil, err
		}
		lifecycle.Append(fx.StartStopHook(files.Start, files.Stop))
		go func() {
			err := <-files.Failed()
			logger.Error("the file server stopped", "error", err)
			_ = shutdowner.Shutdown(fx.ExitCode(1))
		}()
		return files, nil
	}),
	fx.Invoke(func(*design.Files) {}),
)

// operatorWriterModule serves the placing of the operator credential on that
// queue. It needs a kubeop.Writer.
var operatorWriterModule = fx.Module("operator-writer",
	fx.Invoke(func(worker *p7.Worker, writer kubeop.Writer) {
		worker.RegisterActivity(kubeop.ActivityWrite, writer.Write)
	}),
)

// artifactWorkerModule serves the artifact activities on the queue of this
// host, and applies the retention. It needs p7fx.Worker, a
// p7.WorkerConfig and an artifact.Activities.
var artifactWorkerModule = fx.Module("artifact-worker",
	fx.Invoke(func(lifecycle fx.Lifecycle, worker *p7.Worker, config p7.WorkerConfig, activities artifact.Activities, logger *slog.Logger) {
		worker.RegisterActivity(artifact.ActivitySave, activities.Save)
		worker.RegisterActivity(artifact.ActivityGet, activities.Get)
		worker.RegisterActivity(artifact.ActivityList, activities.List)
		var cancel context.CancelFunc
		done := make(chan struct{})
		lifecycle.Append(fx.Hook{
			OnStart: func(context.Context) error {
				var ctx context.Context
				ctx, cancel = context.WithCancel(context.Background())
				go func() { defer close(done); activities.Store.RunSweeper(ctx, artifact.SweepEvery, logger) }()
				return nil
			},
			OnStop: func(ctx context.Context) error {
				cancel()
				select {
				case <-done:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		})
		logger.Info("artifacts are served", "task_queue", config.TaskQueue)
	}),
)
