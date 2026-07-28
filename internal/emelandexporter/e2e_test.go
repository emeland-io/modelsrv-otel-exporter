package emelandexporter_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"emeland.io/modelsrv-otel-exporter/internal/emelandexporter"
)

var _ = Describe("End-to-end: httpcheck -> exporter -> modelsrv API", func() {
	It("probes a TLS endpoint and produces a finding visible via the HTTP API", func() {
		// 1. Start a local HTTPS server with a certificate expiring in 10 days.
		tlsSrv := startTLSServer(10 * 24 * time.Hour)
		defer tlsSrv.Close()

		// 2. Start the exporter with endpoint_mapping pointing to our server.
		apiInstanceID := "eeeeeeee-1111-2222-3333-444444444444"
		cfg := &emelandexporter.Config{
			ListenAddr:      "localhost:24290",
			ExpiryThreshold: 30 * 24 * time.Hour, // 30 days -> 10 days triggers ExpiringSoon
			EndpointMapping: map[string]string{
				tlsSrv.URL: apiInstanceID,
			},
		}

		exp := emelandexporter.NewExporterForTest(cfg)
		err := exp.Start(context.Background())
		Expect(err).ToNot(HaveOccurred())
		defer exp.Shutdown(context.Background())

		// 3. Build metrics as the httpcheck receiver would produce them.
		//    cert_remaining = 10 days in seconds.
		md := buildMetricsWithURL(tlsSrv.URL, 10*24*3600)
		err = exp.ConsumeMetrics(context.Background(), md)
		Expect(err).ToNot(HaveOccurred())

		// 4. Query the modelsrv HTTP API for findings.
		Eventually(func() int {
			return countFindings("http://localhost:24290")
		}, 2*time.Second, 100*time.Millisecond).Should(BeNumerically(">", 0))

		// Verify the finding is CertificateExpiringSoon (10 days < 30 day threshold).
		findings := getFindings("http://localhost:24290")
		Expect(findings).ToNot(BeEmpty())

		// Check that it references our ApiInstance.
		found := false
		for _, f := range findings {
			desc, _ := f["description"].(string)
			if containsSubstring(desc, apiInstanceID) {
				found = true
			}
		}
		Expect(found).To(BeTrue(), "expected a finding referencing ApiInstance %s", apiInstanceID)
	})
})

// --- helpers ---

// startTLSServer creates a local HTTPS server with a self-signed cert
// that expires in the given duration from now.
func startTLSServer(validFor time.Duration) *httptest.Server {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(validFor),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	certDER, _ := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	cert := tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.StartTLS()
	return srv
}

func countFindings(baseURL string) int {
	return len(getFindings(baseURL))
}

func getFindings(baseURL string) []map[string]any {
	resp, err := http.Get(baseURL + "/api/landscape/findings")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var result []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil
	}
	return result
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && contains(s, substr))
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
