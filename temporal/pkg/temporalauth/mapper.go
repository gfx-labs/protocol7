// Package temporalauth maps verified SPIFFE peers to Temporal namespace roles.
package temporalauth

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/authorization"
)

// Config is deployment policy, independent of Kubernetes and any consumer.
// Exact identities override prefixes. The longest matching prefix wins.
// Permissions are never merged across rules.
type Config struct {
	TrustDomain string           `json:"trust_domain"`
	Identities  map[string]Grant `json:"identities"`
	Prefixes    map[string]Grant `json:"prefixes"`
}

type Grant struct {
	SystemAdmin   bool                `json:"system_admin,omitempty"`
	Namespaces    map[string][]string `json:"namespaces,omitempty"`
	NamespaceAPIs map[string][]string `json:"namespace_apis,omitempty"`
	// NexusEndpoints grants workflow and standalone calls from each source namespace.
	NexusEndpoints map[string][]string `json:"nexus_endpoints,omitempty"`
	// TaskQueues limits the task queues the identity may poll and describe,
	// in every namespace it has a grant in. The other grants say which calls
	// are allowed, and this says which queues those calls may name. In a
	// prefix grant, {id} stands for the rest of the caller's SPIFFE ID after
	// the prefix, which must be one path element: a prefix
	// ".../ns/x/session/" with the queue "p7-session-{id}" lets
	// ".../ns/x/session/abc" poll "p7-session-abc" and no other queue. Empty
	// puts no limit on queues.
	TaskQueues []string `json:"task_queues,omitempty"`
}

// namespaceAPIs contains fully qualified, namespace-scoped workflow RPC names.
type namespaceAPIs map[string]map[string]bool

// extension is what a mapped identity carries beyond Temporal's claims.
type extension struct {
	apis           namespaceAPIs
	nexusEndpoints map[string][]string
	// queues is the template of Grant.TaskQueues until GetClaims has
	// expanded it for the caller. Nil is no limit.
	queues []string
}

type Mapper struct {
	trustDomain spiffeid.TrustDomain
	identities  map[string]*authorization.Claims
	prefixes    map[string]*authorization.Claims
}

func LoadConfig(reader io.Reader) (Config, error) {
	var cfg Config
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("read Temporal authorization mapping: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return cfg, fmt.Errorf("unexpected data after Temporal authorization mapping")
	}
	return cfg, nil
}

func NewMapper(cfg Config) (*Mapper, error) {
	td, err := spiffeid.TrustDomainFromString(cfg.TrustDomain)
	if err != nil || cfg.TrustDomain == "" {
		return nil, fmt.Errorf("invalid trust domain %q", cfg.TrustDomain)
	}
	mapper := &Mapper{trustDomain: td, identities: make(map[string]*authorization.Claims), prefixes: make(map[string]*authorization.Claims)}
	for identity, grant := range cfg.Identities {
		id, err := spiffeid.FromString(identity)
		if err != nil || id.TrustDomain() != td {
			return nil, fmt.Errorf("identity %q is not in trust domain %q", identity, td)
		}
		claims, err := grantClaims(grant)
		if err != nil {
			return nil, fmt.Errorf("identity %s: %w", identity, err)
		}
		mapper.identities[identity] = claims
	}
	for prefix, grant := range cfg.Prefixes {
		id, err := spiffeid.FromString(strings.TrimSuffix(prefix, "/"))
		if err != nil || id.TrustDomain() != td || !strings.HasSuffix(prefix, "/") || id.Path() == "" {
			return nil, fmt.Errorf("invalid identity prefix %q", prefix)
		}
		if grant.SystemAdmin {
			return nil, fmt.Errorf("system admin requires an exact identity, not prefix %q", prefix)
		}
		claims, err := grantClaims(grant)
		if err != nil {
			return nil, fmt.Errorf("prefix %s: %w", prefix, err)
		}
		mapper.prefixes[prefix] = claims
	}
	return mapper, nil
}

func grantClaims(grant Grant) (*authorization.Claims, error) {
	claims := &authorization.Claims{AuthType: "mtls", Namespaces: make(map[string]authorization.Role)}
	if grant.SystemAdmin {
		claims.System = authorization.RoleAdmin
	}
	for namespace, roles := range grant.Namespaces {
		if namespace == "" || namespace == "*" || len(roles) == 0 {
			return nil, fmt.Errorf("namespace grants must name a namespace and roles")
		}
		for _, role := range roles {
			switch role {
			case "reader":
				claims.Namespaces[namespace] |= authorization.RoleReader
			case "writer":
				claims.Namespaces[namespace] |= authorization.RoleWriter
			case "worker":
				claims.Namespaces[namespace] |= authorization.RoleWorker
			case "admin":
				claims.Namespaces[namespace] |= authorization.RoleAdmin
			default:
				return nil, fmt.Errorf("unknown role %q", role)
			}
		}
	}
	permissions := make(namespaceAPIs, len(grant.NamespaceAPIs))
	for namespace, methods := range grant.NamespaceAPIs {
		if namespace == "" || namespace == "*" || len(methods) == 0 {
			return nil, fmt.Errorf("API grants must name a namespace and methods")
		}
		permissions[namespace] = make(map[string]bool, len(methods))
		for _, method := range methods {
			qualified := api.WorkflowServicePrefix + method
			if api.GetMethodMetadata(qualified).Scope != api.ScopeNamespace {
				return nil, fmt.Errorf("API %q is not a namespace-scoped workflow method", method)
			}
			permissions[namespace][qualified] = true
		}
	}
	endpoints := make(map[string][]string, len(grant.NexusEndpoints))
	for namespace, names := range grant.NexusEndpoints {
		if namespace == "" || namespace == "*" || len(names) == 0 {
			return nil, fmt.Errorf("Nexus grants must name a source namespace and endpoints")
		}
		for _, name := range names {
			if name == "" || strings.Contains(name, "*") {
				return nil, fmt.Errorf("Nexus grants must name exact endpoints")
			}
		}
		endpoints[namespace] = slices.Clone(names)
	}
	granted := extension{apis: permissions, nexusEndpoints: endpoints}
	if len(grant.TaskQueues) > 0 {
		for _, queue := range grant.TaskQueues {
			if queue == "" {
				return nil, fmt.Errorf("task queue names must not be empty")
			}
		}
		granted.queues = slices.Clone(grant.TaskQueues)
	}
	claims.Extensions = granted
	return claims, nil
}

// AuthInfoRequired makes missing credentials reach GetClaims and fail closed.
func (*Mapper) AuthInfoRequired() bool { return false }

func (m *Mapper) GetClaims(info *authorization.AuthInfo) (*authorization.Claims, error) {
	if info == nil || info.TLSConnection == nil {
		return nil, fmt.Errorf("verified SPIFFE mTLS is required")
	}
	state := info.TLSConnection.State
	if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return nil, fmt.Errorf("verified SPIFFE certificate chain is required")
	}
	id, err := x509svid.IDFromCert(state.VerifiedChains[0][0])
	if err != nil || id.TrustDomain() != m.trustDomain {
		return nil, fmt.Errorf("peer certificate has no permitted SPIFFE identity")
	}
	identity := id.String()
	grant := m.identities[identity]
	// rest is what follows the prefix that granted, for {id}.
	rest := ""
	if grant == nil {
		longest := 0
		for prefix, candidate := range m.prefixes {
			if strings.HasPrefix(identity, prefix) && len(prefix) > longest {
				grant, longest, rest = candidate, len(prefix), identity[len(prefix):]
			}
		}
	}
	if grant == nil {
		return nil, fmt.Errorf("SPIFFE identity %q has no Temporal grant", identity)
	}
	claims := *grant
	claims.Subject = identity
	claims.Namespaces = make(map[string]authorization.Role, len(grant.Namespaces))
	for namespace, role := range grant.Namespaces {
		claims.Namespaces[namespace] = role
	}
	granted := grant.Extensions.(extension)
	var queues []string
	for _, template := range granted.queues {
		if strings.Contains(template, "{id}") {
			if rest == "" || strings.Contains(rest, "/") {
				return nil, fmt.Errorf("SPIFFE identity %q is not one element below its prefix, which its task queues need", identity)
			}
		}
		queues = append(queues, strings.ReplaceAll(template, "{id}", rest))
	}
	permissions := granted.apis
	cloned := make(namespaceAPIs, len(permissions))
	for namespace, methods := range permissions {
		cloned[namespace] = make(map[string]bool, len(methods))
		for method, allowed := range methods {
			cloned[namespace][method] = allowed
		}
	}
	endpoints := make(map[string][]string, len(granted.nexusEndpoints))
	for namespace, names := range granted.nexusEndpoints {
		endpoints[namespace] = slices.Clone(names)
	}
	claims.Extensions = extension{apis: cloned, queues: queues, nexusEndpoints: endpoints}
	return &claims, nil
}
