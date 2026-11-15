// Package p7fx composes protocol7 processes with go.uber.org/fx. It is
// optional: everything in package p7 is a plain constructor, and this
// package only ties those to an fx application's start and stop.
//
// A consumer's process is a call to Run with the options it needs:
//
//	p7fx.Run(ctx, logger,
//		fx.Supply(p7.WorkerConfig{TaskQueue: "reviews", TemporalAddress: address}),
//		p7fx.Worker,
//		fx.Invoke(func(worker *p7.Worker) { worker.RegisterWorkflow("reviews.Review", Review) }),
//	)
package p7fx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

// TemporalConfig says which Temporal a process talks to.
type TemporalConfig struct {
	Address   string
	Namespace string
	// SPIFFE, when its ServerID is set, connects with mutual TLS. The zero
	// value connects in plaintext.
	SPIFFE p7.TemporalSPIFFE
}

// TemporalClient provides a client.Client from a TemporalConfig, for code
// that starts, signals or queries workflows. Starting the application fails
// if Temporal does not answer. The client is closed when it stops.
var TemporalClient = fx.Provide(newTemporalClient)

func newTemporalClient(lifecycle fx.Lifecycle, config TemporalConfig, logger *slog.Logger) (client.Client, error) {
	temporalClient, closeClient, err := p7.NewTemporalClient(client.Options{
		HostPort:  config.Address,
		Namespace: config.Namespace,
		Logger:    log.NewStructuredLogger(logger),
	}, config.SPIFFE)
	if err != nil {
		return nil, fmt.Errorf("configure the Temporal client for %s: %w", config.Address, err)
	}
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if _, err := temporalClient.CheckHealth(ctx, &client.CheckHealthRequest{}); err != nil {
				return fmt.Errorf("connect to Temporal at %s: %w", config.Address, err)
			}
			return nil
		},
		OnStop: func(context.Context) error {
			return closeClient()
		},
	})
	return temporalClient, nil
}

// Worker provides a *p7.Worker from a p7.WorkerConfig. It polls from the
// application's start to its stop. Register workflows and activities in an
// fx.Invoke: invocations run before anything starts, and an application in
// which nothing asks for the worker does not build it.
var Worker = fx.Provide(newWorker)

func newWorker(lifecycle fx.Lifecycle, config p7.WorkerConfig, logger *slog.Logger) (*p7.Worker, error) {
	if config.Logger == nil {
		config.Logger = logger
	}
	worker, err := p7.NewWorker(config)
	if err != nil {
		return nil, err
	}
	lifecycle.Append(fx.StartStopHook(worker.Start, worker.Stop))
	return worker, nil
}

// Client provides a *p7.Client from a p7.ClientConfig, connected from the
// application's start to its stop.
var Client = fx.Provide(newClient)

func newClient(lifecycle fx.Lifecycle, config p7.ClientConfig) (*p7.Client, error) {
	p7Client, err := p7.NewClient(config)
	if err != nil {
		return nil, err
	}
	lifecycle.Append(fx.StartStopHook(p7Client.Connect, p7Client.Close))
	return p7Client, nil
}

// Run builds an application from options, starts it, and stops it when ctx
// ends or a component asks for shutdown through fx.Shutdowner. Components
// stop in the reverse of the order they started in.
//
// logger is available to every constructor as a *slog.Logger. fx's own
// events are logged to it at debug level. What went wrong is the error Run
// returns.
func Run(ctx context.Context, logger *slog.Logger, options ...fx.Option) error {
	if logger == nil {
		logger = slog.Default()
	}
	app := fx.New(
		fx.Supply(logger),
		fx.WithLogger(func() fxevent.Logger {
			events := &fxevent.SlogLogger{Logger: logger}
			events.UseLogLevel(slog.LevelDebug)
			events.UseErrorLevel(slog.LevelDebug)
			return events
		}),
		fx.Options(options...),
	)
	if err := app.Err(); err != nil {
		return err
	}

	startCtx, cancelStart := context.WithTimeout(ctx, app.StartTimeout())
	defer cancelStart()
	if err := app.Start(startCtx); err != nil {
		return err
	}

	var stopped error
	select {
	case <-ctx.Done():
	case signal := <-app.Wait():
		if signal.ExitCode != 0 {
			stopped = fmt.Errorf("a component stopped the process with exit code %d", signal.ExitCode)
		}
	}

	stopCtx, cancelStop := context.WithTimeout(context.WithoutCancel(ctx), app.StopTimeout())
	defer cancelStop()
	return errors.Join(stopped, app.Stop(stopCtx))
}
