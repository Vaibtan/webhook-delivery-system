package safedial

import (
	"net/http"
	"net/netip"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsBlockedIP(t *testing.T) {
	blocked := []string{
		"127.0.0.1",              // loopback
		"::1",                    // loopback v6
		"169.254.169.254",        // link-local (cloud metadata)
		"::ffff:169.254.169.254", // IPv4-mapped link-local — the classic bypass
		"10.0.0.1",               // private
		"172.16.5.4",             // private
		"192.168.1.1",            // private
		"0.0.0.0",                // unspecified
		"100.64.0.1",             // CGNAT
		"240.0.0.1",              // reserved
		"fc00::1",                // IPv6 ULA (private)
		"fe80::1",                // IPv6 link-local
		"ff02::1",                // IPv6 multicast
		"224.0.0.1",              // IPv4 multicast
		"::ffff:127.0.0.1",       // IPv4-mapped loopback
		"::ffff:10.0.0.1",        // IPv4-mapped private
	}
	for _, s := range blocked {
		addr := netip.MustParseAddr(s)
		assert.Truef(t, isBlockedIP(addr), "%s should be blocked", s)
	}

	allowed := []string{
		"8.8.8.8",
		"1.1.1.1",
		"93.184.216.34", // example.com
		"2606:2800:220:1:248:1893:25c8:1946",
	}
	for _, s := range allowed {
		addr := netip.MustParseAddr(s)
		assert.Falsef(t, isBlockedIP(addr), "%s should be allowed", s)
	}
}

func TestHardenedClientTransportHasNoProxy(t *testing.T) {
	c := NewHardenedClient(0, false)
	tr, ok := c.Transport.(*http.Transport)
	require.True(t, ok)
	assert.Nil(t, tr.Proxy, "Proxy must be nil so deliveries never tunnel around SafeDialContext")
	require.NotNil(t, c.CheckRedirect)
}

func TestCheckRedirect(t *testing.T) {
	httpsReq := &http.Request{URL: &url.URL{Scheme: "https", Host: "example.com"}}
	httpReq := &http.Request{URL: &url.URL{Scheme: "http", Host: "example.com"}}

	// https-only mode.
	cr := checkRedirect(false)
	assert.NoError(t, cr(httpsReq, nil), "https redirect allowed")
	assert.Error(t, cr(httpReq, nil), "https→http downgrade blocked")

	// allowHTTP mode.
	crHTTP := checkRedirect(true)
	assert.NoError(t, crHTTP(httpReq, nil), "http redirect allowed when ALLOW_HTTP_URLS")

	// hop cap: a 4th hop (3 already in via) is rejected.
	via3 := []*http.Request{httpsReq, httpsReq, httpsReq}
	assert.Error(t, cr(httpsReq, via3), "must stop after 3 redirects")
	via2 := []*http.Request{httpsReq, httpsReq}
	assert.NoError(t, cr(httpsReq, via2), "3rd redirect still allowed")
}
