package emelandexporter

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Config holds the configuration for the EmELand OTel exporter.
type Config struct {
	// ListenAddr is the address the embedded modelsrv HTTP API listens on.
	ListenAddr string `mapstructure:"listen_addr"`

	// Subscribers is a list of downstream modelsrv URLs that will receive
	// replicated events from this exporter on startup.
	Subscribers []string `mapstructure:"subscribers"`

	// ExpiryThreshold is how far before certificate expiry a
	// CertificateExpiringSoon finding is raised. Defaults to 30 days.
	ExpiryThreshold time.Duration `mapstructure:"expiry_threshold"`

	// EndpointMapping maps probe endpoint URLs (as they appear in http.url
	// data-point attributes) to ApiInstance UUIDs. This removes the need for
	// an attributes processor in the Collector pipeline -- the exporter
	// resolves the link internally.
	EndpointMapping map[string]string `mapstructure:"endpoint_mapping"`
}

func (c *Config) Validate() error {
	if c.ListenAddr == "" {
		return fmt.Errorf("listen_addr is required")
	}
	// Validate that all endpoint_mapping values are valid UUIDs.
	for url, raw := range c.EndpointMapping {
		if _, err := uuid.Parse(raw); err != nil {
			return fmt.Errorf("endpoint_mapping[%q]: invalid UUID %q: %w", url, raw, err)
		}
	}
	return nil
}

// ParsedEndpointMapping returns the endpoint_mapping with values parsed as UUIDs.
func (c *Config) ParsedEndpointMapping() map[string]uuid.UUID {
	m := make(map[string]uuid.UUID, len(c.EndpointMapping))
	for url, raw := range c.EndpointMapping {
		// Validation already passed, so Parse won't fail.
		m[url], _ = uuid.Parse(raw)
	}
	return m
}
