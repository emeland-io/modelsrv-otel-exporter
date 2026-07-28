package sensor_test

import (
	"net/http"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"emeland.io/modelsrv-otel-exporter/internal/sensor"
)

func TestSensor(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sensor Suite")
}

var _ = Describe("Server", func() {
	var srv *sensor.Server

	AfterEach(func() {
		if srv != nil {
			Expect(srv.Close()).To(Succeed())
			srv = nil
		}
	})

	It("starts and exposes the modelsrv HTTP API", func() {
		var err error
		srv, err = sensor.New("localhost:24299", nil, nil)
		Expect(err).ToNot(HaveOccurred())

		// The nodes endpoint should return 200 (even if empty).
		resp, err := http.Get("http://localhost:24299/api/landscape/nodes")
		Expect(err).ToNot(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
	})

	It("registers well-known certificate FindingTypes", func() {
		var err error
		srv, err = sensor.New("localhost:24298", nil, nil)
		Expect(err).ToNot(HaveOccurred())

		resp, err := http.Get("http://localhost:24298/api/landscape/findingTypes")
		Expect(err).ToNot(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
	})
})
