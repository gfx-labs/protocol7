package domain_test

import (
	"strings"
	"testing"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

func TestValidPool(t *testing.T) {
	for _, good := range []string{"0123456789abcdef", "abcdefgh", strings.Repeat("a", 32)} {
		if err := domain.ValidPool(good); err != nil {
			t.Errorf("%q: %v", good, err)
		}
	}
	for _, bad := range []string{"", "short", strings.Repeat("a", 33), "ABCDEFGHIJ", "0123/56789", "0123456789ab-def", "0123456789 abcd", "../../abcdef01"} {
		if domain.ValidPool(bad) == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
}

func TestPoolSpecValidate(t *testing.T) {
	good := domain.PoolSpec{Name: "0123456789abcdef", MaxWorkers: 3, SessionsPerWorker: 3}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	if good.Idle() != domain.DefaultPoolIdleTimeout || (domain.PoolSpec{IdleTimeout: time.Second}).Idle() != time.Second {
		t.Error("Idle is not the timeout, or its default")
	}
	for name, change := range map[string]func(*domain.PoolSpec){
		"no workers":        func(s *domain.PoolSpec) { s.MaxWorkers = 0 },
		"too many":          func(s *domain.PoolSpec) { s.MaxWorkers = domain.MaxPoolWorkers + 1 },
		"no sessions":       func(s *domain.PoolSpec) { s.SessionsPerWorker = 0 },
		"negative idle":     func(s *domain.PoolSpec) { s.IdleTimeout = -1 },
		"a name that isn't": func(s *domain.PoolSpec) { s.Name = "x" },
	} {
		spec := good
		change(&spec)
		if spec.Validate() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	spec := good
	spec.MaxWorkers = domain.MaxPoolWorkers
	if err := spec.Validate(); err != nil {
		t.Errorf("the most workers: %v", err)
	}
}

func TestPoolNames(t *testing.T) {
	if got := domain.PoolQueue("0123456789abcdef", 3); got != "p7-pool-0123456789abcdef-3" || domain.PoolPodName("0123456789abcdef", 3) != got {
		t.Error(got)
	}
	if got := domain.PoolWorkflowID("0123456789abcdef"); got != "p7.pool/0123456789abcdef" {
		t.Error(got)
	}
	for prefix, want := range map[string]string{"sessions": "sessions/pools/g", "/sessions/": "sessions/pools/g", "": "pools/g", "a/b": "a/b/pools/g"} {
		if got := domain.PoolSnapshotPrefix(prefix, "g"); got != want {
			t.Errorf("prefix %q: %q, want %q", prefix, got, want)
		}
	}
}
