// Package signature implements the canonical HMAC-SHA256 signing scheme used in
// both directions (inbound verification and outbound delivery), plus
// deterministic JSON canonicalization.
//
// One canonical grammar, used everywhere:
//
//	signed_message = timestamp + "." + idempotency_key + "." + body
//
// idempotency_key is empty when absent (always empty outbound); its charset
// excludes '.', so the field boundaries are unambiguous.
package signature

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
)

const sigPrefix = "sha256="

// BuildMessage assembles the canonical signing input from its three independent
// fields. The verifier reconstructs the message the same way (it never parses
// wire bytes back apart), so the '.'-free key charset makes boundaries exact.
func BuildMessage(timestamp, idempotencyKey string, body []byte) []byte {
	out := make([]byte, 0, len(timestamp)+len(idempotencyKey)+len(body)+2)
	out = append(out, timestamp...)
	out = append(out, '.')
	out = append(out, idempotencyKey...)
	out = append(out, '.')
	out = append(out, body...)
	return out
}

// Sign returns "sha256=<hex>" — the HMAC-SHA256 of message under secret.
func Sign(message []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(message)
	return sigPrefix + hex.EncodeToString(mac.Sum(nil))
}

// Verify reports whether signature matches Sign(message, secret), in constant
// time (hmac.Equal). signature is the full "sha256=<hex>" form.
func Verify(message []byte, secret, signature string) bool {
	expected := Sign(message, secret)
	return hmac.Equal([]byte(expected), []byte(signature))
}

// ValidIdempotencyKey enforces [A-Za-z0-9_-]{1,128} so a client key can never
// contain the '.' signing delimiter. An empty key is "absent", not invalid.
func ValidIdempotencyKey(k string) bool {
	if k == "" {
		return true
	}
	if len(k) > 128 {
		return false
	}
	for i := 0; i < len(k); i++ {
		c := k[i]
		switch {
		case c >= 'A' && c <= 'Z':
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '_' || c == '-':
		default:
			return false
		}
	}
	return true
}

// CanonicalJSON re-serializes raw JSON deterministically: numbers preserved via
// UseNumber (no float64 rounding) and object keys sorted (encoding/json marshals
// map keys in sorted order). It REJECTS trailing tokens after the first value —
// the well-defined check is a second Decode that must return io.EOF (dec.More()
// reports array/object continuation, not stream end).
func CanonicalJSON(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var tmp any
	if err := dec.Decode(&tmp); err != nil {
		return nil, err
	}
	var rest json.RawMessage
	if err := dec.Decode(&rest); !errors.Is(err, io.EOF) {
		return nil, errors.New("signature: unexpected trailing data after JSON value")
	}
	return json.Marshal(tmp)
}
