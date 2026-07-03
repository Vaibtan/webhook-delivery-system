// Package safedial provides SSRF-resistant outbound dialing and a hardened
// http.Client. Destination hostnames are resolved and every candidate IP is
// classified against blocked ranges before any connection is made. See plan §6.
package safedial

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"time"
)

// blockedPrefixes are the ranges not already covered by the netip Is* helpers.
var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),     // "this host on this network"
	netip.MustParsePrefix("100.64.0.0/10"), // CGNAT (RFC 6598)
	netip.MustParsePrefix("240.0.0.0/4"),   // reserved
}

// isBlockedIP reports whether addr is in a private/loopback/link-local/reserved
// range. IPv4-mapped IPv6 (::ffff:a.b.c.d) is collapsed via Unmap first — a
// classic SSRF bypass.
func isBlockedIP(addr netip.Addr) bool {
	addr = addr.Unmap()
	if !addr.IsValid() {
		return true // unparseable ⇒ fail closed
	}
	if addr.IsLoopback() || addr.IsPrivate() || addr.IsLinkLocalUnicast() ||
		addr.IsLinkLocalMulticast() || addr.IsMulticast() || addr.IsUnspecified() {
		return true // IsLinkLocalUnicast covers 169.254.0.0/16 (incl. 169.254.169.254)
	}
	for _, p := range blockedPrefixes {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// SafeDialContext resolves addr, blocks if ANY resolved IP is internal, and pins
// the connection to the first vetted IP (so a re-resolution can't swap in a
// private address between check and dial — DNS rebinding). Fails closed on an
// empty DNS answer.
func SafeDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("safedial: no IPs resolved for %s", host)
	}
	for _, ip := range ips {
		if isBlockedIP(ip) {
			return nil, fmt.Errorf("safedial: connection to %s (%s) blocked", ip, host)
		}
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0].String(), port))
}

// NewHardenedClient builds the shared outbound http.Client: SafeDialContext for
// SSRF protection, Proxy:nil (never tunnel around the safe dialer via proxy env
// vars), and a CheckRedirect that caps hops at 3 and forbids https→http
// downgrades unless allowHTTP. Each redirect hop re-dials through SafeDialContext,
// so a 30x to an internal host is still blocked.
func NewHardenedClient(timeout time.Duration, allowHTTP bool) *http.Client {
	transport := &http.Transport{
		Proxy:               nil,
		DialContext:         SafeDialContext,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	return &http.Client{
		Timeout:       timeout,
		Transport:     transport,
		CheckRedirect: checkRedirect(allowHTTP),
	}
}

func checkRedirect(allowHTTP bool) func(req *http.Request, via []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return errors.New("safedial: stopped after 3 redirects")
		}
		switch req.URL.Scheme {
		case "https":
			return nil
		case "http":
			if allowHTTP {
				return nil
			}
			return errors.New("safedial: refusing redirect to non-https URL")
		default:
			return fmt.Errorf("safedial: refusing redirect to scheme %q", req.URL.Scheme)
		}
	}
}
