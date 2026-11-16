package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"time"
)

// Leader election: one `p7 pods` replica reconciles. The replicas campaign
// for a coordination.k8s.io Lease in the session pods' namespace. The holder
// renews it every few seconds. A holder that stops releases the lease and
// another replica takes over at once; one that dies leaves it to expire, and
// another takes over after Config.LeaseDuration. The service account needs
// get, create and update on leases of that namespace.
//
// This is the protocol of client-go's leaderelection package, on the plain
// HTTP calls the rest of the backend makes. client-go itself adds 34 MB to
// the binary, which has no other use for it.
//
// A replica never reads the clocks in the lease. It takes the lease from
// another holder only when the record has not changed for a whole lease
// duration by its own clock, and a holder gives up when it has not renewed
// for the shorter renew deadline, so the two never lead at once, however
// their clocks differ. Writes carry the lease's resourceVersion, so two
// replicas that race for an expired lease cannot both win.

const (
	defaultLeaseName     = "p7-pods-reconciler"
	defaultLeaseDuration = 15 * time.Second
)

// leaseTiming derives the renew deadline and the retry period from the lease
// duration, in the ratios of client-go's defaults (15s, 10s and 2s).
func leaseTiming(duration time.Duration) (lease, renew, retry time.Duration) {
	if duration <= 0 {
		duration = defaultLeaseDuration
	}
	return duration, duration * 2 / 3, duration * 2 / 15
}

// newIdentity names this replica in the lease: the host name, which is the
// pod's, and a random suffix so that a restarted pod never mistakes itself
// for its predecessor.
func newIdentity() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s_%08x", host, rand.Uint32())
}

// leaseMicroTime is the format of a Lease's acquireTime and renewTime.
const leaseMicroTime = "2006-01-02T15:04:05.000000Z07:00"

type leaseObject struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name            string `json:"name"`
		Namespace       string `json:"namespace,omitempty"`
		ResourceVersion string `json:"resourceVersion,omitempty"`
	} `json:"metadata"`
	Spec leaseSpec `json:"spec"`
}

type leaseSpec struct {
	HolderIdentity       string `json:"holderIdentity"`
	LeaseDurationSeconds int    `json:"leaseDurationSeconds,omitempty"`
	AcquireTime          string `json:"acquireTime,omitempty"`
	RenewTime            string `json:"renewTime,omitempty"`
	LeaseTransitions     int    `json:"leaseTransitions"`
}

// elector is one replica's candidacy.
type elector struct {
	pods     *Pods
	logger   *slog.Logger
	name     string
	path     string
	identity string
	now      func() time.Time
	// lease is how long the lease holds without a renewal, renew how long
	// the holder tries before it gives up, and retry how often.
	lease, renew, retry time.Duration
	// observed is the last record read, and observedAt when this replica
	// saw it change.
	observed   leaseSpec
	observedAt time.Time
}

func (p *Pods) newElector() *elector {
	e := &elector{pods: p, name: p.config.LeaseName, now: time.Now, identity: p.identity, logger: p.logger}
	if e.name == "" {
		e.name = defaultLeaseName
	}
	e.lease, e.renew, e.retry = leaseTiming(p.config.LeaseDuration)
	e.path = "/apis/coordination.k8s.io/v1/namespaces/" + p.config.Namespace + "/leases"
	return e
}

// campaign runs for the lease until ctx ends. Each time it holds the lease it
// runs lead, with a context that ends when the lease is lost or ctx ends, and
// waits for lead to return before it gives the lease up or campaigns again.
func (e *elector) campaign(ctx context.Context, lead func(context.Context)) {
	for ctx.Err() == nil {
		if !e.acquire(ctx) {
			return
		}
		e.logger.Info("session pod reconciler elected", "identity", e.identity, "lease", e.name)
		leadCtx, cancel := context.WithCancel(ctx)
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			lead(leadCtx)
		}()
		e.renewUntilLost(leadCtx)
		cancel()
		<-finished
		if ctx.Err() != nil {
			e.release()
		}
		e.logger.Info("session pod reconciler stopped leading", "identity", e.identity, "lease", e.name)
	}
}

// jittered returns the retry period, plus up to 20 percent so that replicas
// do not try in step.
func (e *elector) jittered() time.Duration {
	return e.retry + time.Duration(rand.Float64()*0.2*float64(e.retry))
}

// acquire tries every retry period until it holds the lease. It is false if
// ctx ended first.
func (e *elector) acquire(ctx context.Context) bool {
	for {
		if e.try(ctx, e.lease) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(e.jittered()):
		}
	}
}

// renewUntilLost renews the lease every retry period and returns when it has
// not succeeded for the renew deadline, or ctx ends.
func (e *elector) renewUntilLost(ctx context.Context) {
	renewed := e.now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.retry):
		}
		left := e.renew - e.now().Sub(renewed)
		if left <= 0 {
			e.logger.Warn("session pod reconciler could not renew its lease", "identity", e.identity, "lease", e.name)
			return
		}
		if e.try(ctx, left) {
			renewed = e.now()
		}
	}
}

// try reads the lease and takes it, renews it, or finds it held. It is true
// when this replica holds the lease afterwards. The calls end after the
// timeout.
func (e *elector) try(ctx context.Context, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lease leaseObject
	err := e.pods.do(ctx, http.MethodGet, e.path+"/"+e.name, nil, &lease)
	if isStatus(err, http.StatusNotFound) {
		lease = leaseObject{APIVersion: "coordination.k8s.io/v1", Kind: "Lease"}
		lease.Metadata.Name, lease.Metadata.Namespace = e.name, e.pods.config.Namespace
		lease.Spec = e.claim(leaseSpec{})
		var created leaseObject
		if err := e.pods.do(ctx, http.MethodPost, e.path, lease, &created); err != nil {
			e.failed("create the lease", err)
			return false
		}
		e.observe(created)
		return true
	}
	if err != nil {
		e.failed("read the lease", err)
		return false
	}
	// A change to the record restarts the clock on its holder. Only a
	// record that stays as it is for a whole lease duration is expired.
	if lease.Spec.HolderIdentity != e.observed.HolderIdentity || lease.Spec.RenewTime != e.observed.RenewTime ||
		lease.Spec.AcquireTime != e.observed.AcquireTime || lease.Spec.LeaseTransitions != e.observed.LeaseTransitions {
		e.observe(lease)
	}
	holder := lease.Spec.HolderIdentity
	if holder != "" && holder != e.identity {
		duration := time.Duration(lease.Spec.LeaseDurationSeconds) * time.Second
		if duration <= 0 {
			duration = e.lease
		}
		if e.now().Before(e.observedAt.Add(duration)) {
			return false
		}
	}
	lease.Spec = e.claim(lease.Spec)
	var updated leaseObject
	if err := e.pods.do(ctx, http.MethodPut, e.path+"/"+e.name, lease, &updated); err != nil {
		// A conflict is another replica that won the race.
		if !isStatus(err, http.StatusConflict) {
			e.failed("update the lease", err)
		}
		return false
	}
	e.observe(updated)
	return true
}

// claim returns the spec of the lease with this replica as its holder, as of
// now.
func (e *elector) claim(spec leaseSpec) leaseSpec {
	now := e.now().UTC().Format(leaseMicroTime)
	if spec.HolderIdentity != e.identity {
		if spec.HolderIdentity != "" {
			spec.LeaseTransitions++
		}
		spec.AcquireTime = now
	}
	spec.HolderIdentity = e.identity
	spec.LeaseDurationSeconds = int((e.lease + time.Second - 1) / time.Second)
	spec.RenewTime = now
	return spec
}

func (e *elector) observe(lease leaseObject) {
	e.observed, e.observedAt = lease.Spec, e.now()
}

func (e *elector) failed(what string, err error) {
	if !errors.Is(err, context.Canceled) {
		e.logger.Warn("session pod reconciler lease", "action", what, "identity", e.identity, "error", err)
	}
}

// release gives the lease up, so that another replica leads without waiting
// for it to expire. It fails quietly: the lease then expires.
func (e *elector) release() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var lease leaseObject
	if err := e.pods.do(ctx, http.MethodGet, e.path+"/"+e.name, nil, &lease); err != nil || lease.Spec.HolderIdentity != e.identity {
		return
	}
	lease.Spec.HolderIdentity = ""
	lease.Spec.LeaseDurationSeconds = 1
	lease.Spec.RenewTime = e.now().UTC().Format(leaseMicroTime)
	_ = e.pods.do(ctx, http.MethodPut, e.path+"/"+e.name, lease, nil)
}
