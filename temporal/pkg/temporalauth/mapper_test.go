package temporalauth_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gfx-labs/protocol7/temporal/pkg/temporalauth"
	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/authorization"
	"google.golang.org/grpc/credentials"
)

const trustDomain = "test.example"
const serverIdentity = "spiffe://test.example/ns/core/sa/temporal"

func TestMTLSNamespaceAuthorization(t *testing.T) {
	mapping, err := temporalauth.LoadConfig(strings.NewReader(`{
		"trust_domain":"test.example",
		"identities":{
			"spiffe://test.example/ns/core/sa/temporal":{"system_admin":true},
			"spiffe://test.example/ns/core/sa/worker":{"namespaces":{"default":["worker"]}},
			"spiffe://test.example/ns/core/sa/compatible-worker":{"namespaces":{"default":["worker"]},"namespace_apis":{"default":["UpdateWorkflowExecution","PollWorkflowExecutionUpdate","SignalWithStartWorkflowExecution"]}},
			"spiffe://test.example/ns/project-a/sa/worker":{"namespaces":{"project-a":["worker"]},"namespace_apis":{"project-a":["UpdateWorkflowExecution","PollWorkflowExecutionUpdate","SignalWithStartWorkflowExecution"]}},
			"spiffe://test.example/ns/core/sa/pods":{"namespaces":{"default":["worker"]},"namespace_apis":{"default":["StartWorkflowExecution","DescribeWorkflowExecution"]}},
			"spiffe://test.example/ns/navi/sa/navi":{"namespaces":{"default":["writer"],"project-a":["writer"]}}
		},
		"prefixes":{
			"spiffe://test.example/ns/core/sa/":{"namespaces":{"default":["writer","worker"]}},
			"spiffe://test.example/ns/project-a/sa/":{"namespaces":{"project-a":["writer","worker"]}},
			"spiffe://test.example/ns/project-a/sa/readers/":{"namespaces":{"project-a":["reader"]}}
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	mapper, err := temporalauth.NewMapper(mapping)
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
	tests := []struct {
		name      string
		identity  string
		namespace string
		method    string
		allow     bool
	}{
		{"core writer starts", "core/sa/selfdev", "default", "StartWorkflowExecution", true},
		{"core writer signals", "core/sa/selfdev", "default", "SignalWorkflowExecution", true},
		{"core writer isolated", "core/sa/selfdev", "project-a", "StartWorkflowExecution", false},
		{"project writer", "project-a/sa/service", "project-a", "SignalWithStartWorkflowExecution", true},
		{"project isolated", "project-a/sa/service", "default", "SignalWorkflowExecution", false},
		{"project reader", "project-a/sa/readers/user", "project-a", "ListWorkflowExecutions", true},
		{"longest prefix does not merge", "project-a/sa/readers/user", "project-a", "StartWorkflowExecution", false},
		{"navi core writer", "navi/sa/navi", "default", "StartWorkflowExecution", true},
		{"navi project writer", "navi/sa/navi", "project-a", "SignalWorkflowExecution", true},
		{"navi unlisted project", "navi/sa/navi", "project-b", "StartWorkflowExecution", false},
		{"navi not admin", "navi/sa/navi", "project-a", "RegisterNamespace", false},
		{"server system admin", "core/sa/temporal", "project-b", "RegisterNamespace", true},
		{"worker polls", "core/sa/worker", "default", "PollActivityTaskQueue", true},
		{"worker responds", "core/sa/worker", "default", "RespondActivityTaskCompleted", true},
		{"worker heartbeats", "core/sa/worker", "default", "RecordActivityTaskHeartbeat", true},
		{"worker history", "core/sa/worker", "default", "GetWorkflowExecutionHistory", true},
		{"worker other namespace", "core/sa/worker", "project-a", "PollActivityTaskQueue", false},
		{"owner tool and permission updates", "core/sa/compatible-worker", "default", "UpdateWorkflowExecution", true},
		{"owner update completion", "core/sa/compatible-worker", "default", "PollWorkflowExecutionUpdate", true},
		{"review filing", "core/sa/compatible-worker", "default", "SignalWithStartWorkflowExecution", true},
		{"compatible worker cannot direct start", "core/sa/compatible-worker", "default", "StartWorkflowExecution", false},
		{"compatible worker cannot direct signal", "core/sa/compatible-worker", "default", "SignalWorkflowExecution", false},
		{"compatible worker cannot terminate", "core/sa/compatible-worker", "default", "TerminateWorkflowExecution", false},
		{"compatible worker cannot reset", "core/sa/compatible-worker", "default", "ResetWorkflowExecution", false},
		{"compatible worker cannot delete", "core/sa/compatible-worker", "default", "DeleteWorkflowExecution", false},
		{"compatible worker cannot create namespace", "core/sa/compatible-worker", "default", "RegisterNamespace", false},
		{"compatible worker cannot schedule", "core/sa/compatible-worker", "default", "CreateSchedule", false},
		{"compatible worker cannot multi operation", "core/sa/compatible-worker", "default", "ExecuteMultiOperation", false},
		{"core updates cannot escape", "core/sa/compatible-worker", "project-a", "UpdateWorkflowExecution", false},
		{"project update", "project-a/sa/worker", "project-a", "UpdateWorkflowExecution", true},
		{"project update cannot escape", "project-a/sa/worker", "default", "UpdateWorkflowExecution", false},
		{"project signal with start cannot escape", "project-a/sa/worker", "default", "SignalWithStartWorkflowExecution", false},
		{"pod reconciler probe start", "core/sa/pods", "default", "StartWorkflowExecution", true},
		{"pod reconciler owner describe", "core/sa/pods", "default", "DescribeWorkflowExecution", true},
		{"pod reconciler cannot update owner", "core/sa/pods", "default", "UpdateWorkflowExecution", false},
	}
	for _, method := range []string{"StartWorkflowExecution", "SignalWorkflowExecution", "SignalWithStartWorkflowExecution", "TerminateWorkflowExecution", "ResetWorkflowExecution", "RequestCancelWorkflowExecution", "UpdateWorkflowExecution", "ExecuteMultiOperation", "CreateSchedule", "RegisterNamespace", "ListWorkflowExecutions"} {
		tests = append(tests, struct {
			name, identity, namespace, method string
			allow                             bool
		}{"worker cannot " + method, "core/sa/worker", "default", method, false})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			svid := makeSVID(t, ca, key, "spiffe://test.example/ns/"+test.identity)
			cfg := temporalauth.ClientTLSConfig(svid, bundle, serverSVID.ID)
			claims, err := exchange(t, frontend, cfg, mapper)
			if err != nil {
				t.Fatal(err)
			}
			result, err := authorizer.Authorize(context.Background(), claims, &authorization.CallTarget{Namespace: test.namespace, APIName: api.WorkflowServicePrefix + test.method})
			if err != nil {
				t.Fatal(err)
			}
			if (result.Decision == authorization.DecisionAllow) != test.allow {
				t.Fatalf("%s in %s: got %v, allow=%v", test.method, test.namespace, result.Decision, test.allow)
			}
		})
	}
	for _, test := range []struct {
		identity, target string
		allow            bool
	}{
		{"core/sa/worker", "default", false},
		{"core/sa/worker", "", false},
		{"core/sa/compatible-worker", "default", false},
		{"project-a/sa/worker", "default", false},
		{"core/sa/selfdev", "default", true},
		{"core/sa/selfdev", "project-a", false},
	} {
		t.Run("embedded signal "+test.identity+" to "+test.target, func(t *testing.T) {
			svid := makeSVID(t, ca, key, "spiffe://test.example/ns/"+test.identity)
			claims, err := exchange(t, frontend, temporalauth.ClientTLSConfig(svid, bundle, serverSVID.ID), mapper)
			if err != nil {
				t.Fatal(err)
			}
			request := &workflowservice.RespondWorkflowTaskCompletedRequest{Namespace: "default", Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION,
				Attributes:  &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{Namespace: test.target}},
			}}}
			result, err := authorizer.Authorize(context.Background(), claims, &authorization.CallTarget{Namespace: "default", APIName: api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted", Request: request})
			if err != nil {
				t.Fatal(err)
			}
			if (result.Decision == authorization.DecisionAllow) != test.allow {
				t.Fatalf("embedded signal decision %v, allow=%v", result.Decision, test.allow)
			}
		})
	}
	for _, test := range []struct {
		identity, target string
		allow            bool
	}{
		{"core/sa/compatible-worker", "default", false},
		{"core/sa/selfdev", "default", true},
		{"core/sa/pods", "default", true},
		{"core/sa/pods", "project-a", false},
	} {
		t.Run("embedded child "+test.identity+" to "+test.target, func(t *testing.T) {
			svid := makeSVID(t, ca, key, "spiffe://test.example/ns/"+test.identity)
			claims, err := exchange(t, frontend, temporalauth.ClientTLSConfig(svid, bundle, serverSVID.ID), mapper)
			if err != nil {
				t.Fatal(err)
			}
			request := &workflowservice.RespondWorkflowTaskCompletedRequest{Namespace: "default", Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes:  &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{Namespace: test.target}},
			}}}
			result, err := authorizer.Authorize(context.Background(), claims, &authorization.CallTarget{Namespace: "default", APIName: api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted", Request: request})
			if err != nil {
				t.Fatal(err)
			}
			if (result.Decision == authorization.DecisionAllow) != test.allow {
				t.Fatalf("embedded child decision %v, allow=%v", result.Decision, test.allow)
			}
		})
	}
	for _, test := range []struct{ namespace, method string }{{"default", "ListNamespaces"}, {"default", "NotAnRPC"}, {"*", "StartWorkflowExecution"}} {
		t.Run("reject unsafe API policy "+test.namespace+" "+test.method, func(t *testing.T) {
			input := fmt.Sprintf(`{"trust_domain":"test.example","identities":{"%s":{"namespace_apis":{"%s":["%s"]}}}}`, serverIdentity, test.namespace, test.method)
			cfg, err := temporalauth.LoadConfig(strings.NewReader(input))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := temporalauth.NewMapper(cfg); err == nil {
				t.Fatal("accepted wildcard, unknown or cluster API grant")
			}
		})
	}
	for _, identity := range []string{"spiffe://test.example/ns/unknown/sa/user", "spiffe://other.example/ns/core/sa/selfdev", "spiffe://test.example/ns/core-lookalike/sa/selfdev"} {
		t.Run("reject "+identity, func(t *testing.T) {
			svid := makeSVID(t, ca, key, identity)
			cfg := temporalauth.ClientTLSConfig(svid, bundle, serverSVID.ID)
			if _, err := exchange(t, frontend, cfg, mapper); err == nil {
				t.Fatal("unexpected authorized identity")
			}
		})
	}
	t.Run("missing client certificate", func(t *testing.T) {
		cfg := tlsconfig.TLSClientConfig(bundle, tlsconfig.AuthorizeID(serverSVID.ID))
		if _, err := exchange(t, frontend, cfg, mapper); err == nil {
			t.Fatal("accepted anonymous client")
		}
	})
	t.Run("untrusted client certificate", func(t *testing.T) {
		otherCA, otherKey := makeCA(t)
		svid := makeSVID(t, otherCA, otherKey, "spiffe://test.example/ns/core/sa/selfdev")
		cfg := temporalauth.ClientTLSConfig(svid, bundle, serverSVID.ID)
		if _, err := exchange(t, frontend, cfg, mapper); err == nil {
			t.Fatal("accepted untrusted client")
		}
	})
	t.Run("internal listener excludes project", func(t *testing.T) {
		internal, err := provider.GetInternodeServerConfig()
		if err != nil {
			t.Fatal(err)
		}
		svid := makeSVID(t, ca, key, "spiffe://test.example/ns/project-a/sa/service")
		cfg := temporalauth.ClientTLSConfig(svid, bundle, serverSVID.ID)
		if _, err := exchange(t, internal, cfg, mapper); err == nil {
			t.Fatal("project reached internal frontend")
		}
		cfg, err = provider.GetInternodeClientConfig()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := exchange(t, internal, cfg, mapper); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("reject wrong server identity", func(t *testing.T) {
		cfg := temporalauth.ClientTLSConfig(serverSVID, bundle, spiffeid.RequireFromString("spiffe://test.example/ns/other/sa/temporal"))
		if _, err := exchange(t, frontend, cfg, mapper); err == nil {
			t.Fatal("accepted wrong server identity")
		}
	})
	if _, err := mapper.GetClaims(nil); err == nil {
		t.Fatal("accepted missing TLS info")
	}
	if _, err := mapper.GetClaims(&authorization.AuthInfo{TLSConnection: &credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: serverSVID.Certificates}}}); err == nil {
		t.Fatal("accepted unverified peer certificates")
	}
}

// exchange crosses a real TLS 1.3 connection, then passes the server's verified
// connection state to the same mapper used by the Temporal server.
func exchange(t *testing.T, serverConfig, clientConfig *tls.Config, mapper *temporalauth.Mapper) (*authorization.Claims, error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	type outcome struct {
		claims *authorization.Claims
		err    error
	}
	result := make(chan outcome, 1)
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			result <- outcome{err: err}
			return
		}
		defer raw.Close()
		_ = raw.SetDeadline(time.Now().Add(3 * time.Second))
		connection := tls.Server(raw, serverConfig)
		if err := connection.Handshake(); err != nil {
			result <- outcome{err: err}
			return
		}
		if connection.ConnectionState().NegotiatedProtocol != "h2" {
			result <- outcome{err: fmt.Errorf("gRPC connection did not negotiate HTTP/2")}
			return
		}
		claims, err := mapper.GetClaims(&authorization.AuthInfo{TLSConnection: &credentials.TLSInfo{State: connection.ConnectionState()}})
		if err == nil {
			_, err = connection.Write([]byte{1})
		}
		result <- outcome{claims: claims, err: err}
	}()
	connection, clientErr := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", listener.Addr().String(), clientConfig)
	if clientErr == nil {
		_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
		_, clientErr = io.ReadFull(connection, make([]byte, 1))
		_ = connection.Close()
	}
	serverResult := <-result
	if serverResult.err != nil {
		return nil, serverResult.err
	}
	if clientErr != nil {
		return nil, clientErr
	}
	return serverResult.claims, nil
}

func makeCA(t *testing.T) (*x509.Certificate, ed25519.PrivateKey) {
	t.Helper()
	public, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func makeSVID(t *testing.T, ca *x509.Certificate, caKey ed25519.PrivateKey, identity string) *x509svid.SVID {
	t.Helper()
	public, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse(identity)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: serial, URIs: []*url.URL{uri}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, public, caKey)
	if err != nil {
		t.Fatal(err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	svid, err := x509svid.ParseRaw(der, privateDER)
	if err != nil {
		t.Fatal(fmt.Errorf("parse SVID %s: %w", identity, err))
	}
	return svid
}
