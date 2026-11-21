package temporalauth_test

import (
	"context"
	"crypto/x509"
	"strings"
	"testing"

	"github.com/gfx-labs/protocol7/temporal/pkg/temporalauth"
	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/authorization"
)

// Endpoint authorization belongs at command acceptance because subsequent
// worker-target dispatch uses the server's identity, not the caller's identity.
func TestNexusEndpointGrants(t *testing.T) {
	config, err := temporalauth.LoadConfig(strings.NewReader(`{
		"trust_domain":"test.example",
		"identities":{
			"spiffe://test.example/ns/core/sa/temporal":{"system_admin":true},
			"spiffe://test.example/ns/caller/sa/worker":{
				"namespaces":{"caller":["writer","worker"],"other":["writer","worker"]},
				"nexus_endpoints":{"caller":["review"]}
			},
			"spiffe://test.example/ns/denied/sa/worker":{"namespaces":{"caller":["writer","worker"]}},
			"spiffe://test.example/ns/no-role/sa/worker":{"nexus_endpoints":{"caller":["review"]}}
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	mapper, err := temporalauth.NewMapper(config)
	if err != nil {
		t.Fatal(err)
	}
	ca, key := makeCA(t)
	bundle := x509bundle.FromX509Authorities(spiffeid.RequireTrustDomainFromString(trustDomain), []*x509.Certificate{ca})
	serverSVID := makeSVID(t, ca, key, serverIdentity)
	provider, err := temporalauth.NewTLSProvider(serverSVID, bundle, serverSVID.ID)
	if err != nil {
		t.Fatal(err)
	}
	frontend, err := provider.GetFrontendServerConfig()
	if err != nil {
		t.Fatal(err)
	}
	authorizer := temporalauth.NewAuthorizer()
	for _, test := range []struct {
		name, identity, namespace, endpoint string
		allow                               bool
	}{
		{"allowed", "caller", "caller", "review", true},
		{"another endpoint", "caller", "caller", "review-other", false},
		{"empty endpoint", "caller", "caller", "", false},
		{"another source namespace", "caller", "other", "review", false},
		{"another identity", "denied", "caller", "review", false},
		{"endpoint grant grants no API", "no-role", "caller", "review", false},
		{"system administrator", "core", "other", "review-other", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			account := "worker"
			if test.identity == "core" {
				account = "temporal"
			}
			svid := makeSVID(t, ca, key, "spiffe://test.example/ns/"+test.identity+"/sa/"+account)
			claims, err := exchange(t, frontend, temporalauth.ClientTLSConfig(svid, bundle, serverSVID.ID), mapper)
			if err != nil {
				t.Fatal(err)
			}
			command := &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{Endpoint: test.endpoint},
				},
			}
			for _, target := range []*authorization.CallTarget{
				{Namespace: test.namespace, APIName: api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted", Request: &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: []*commandpb.Command{command}}},
				{Namespace: test.namespace, APIName: api.WorkflowServicePrefix + "StartNexusOperationExecution", Request: &workflowservice.StartNexusOperationExecutionRequest{Endpoint: test.endpoint}},
			} {
				result, err := authorizer.Authorize(context.Background(), claims, target)
				if err != nil {
					t.Fatal(err)
				}
				if (result.Decision == authorization.DecisionAllow) != test.allow {
					t.Fatalf("%s: decision %v, want allowed=%v", target.APIName, result.Decision, test.allow)
				}
			}
			// A namespace writer cannot bypass endpoint checks through the HTTP route.
			result, err := authorizer.Authorize(context.Background(), claims, &authorization.CallTarget{Namespace: test.namespace, APIName: api.NexusServicePrefix + "DispatchByEndpoint", NexusEndpointName: test.endpoint})
			if err != nil {
				t.Fatal(err)
			}
			if (result.Decision == authorization.DecisionAllow) != (test.identity == "core") {
				t.Fatalf("direct HTTP dispatch: decision %v", result.Decision)
			}
		})
	}
	for _, grant := range []string{`{"*":["review"]}`, `{"caller":["*"]}`, `{"caller":[""]}`, `{"caller":[]}`, `{"": ["review"]}`} {
		cfg, err := temporalauth.LoadConfig(strings.NewReader(`{"trust_domain":"test.example","identities":{"` + serverIdentity + `":{"nexus_endpoints":` + grant + `}}}`))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := temporalauth.NewMapper(cfg); err == nil {
			t.Fatalf("accepted unsafe endpoint grant %s", grant)
		}
	}
}
