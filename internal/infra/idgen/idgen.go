// Package idgen produces cryptographically-random identifiers and secrets using
// only crypto/rand (stdlib). UUIDs are RFC 4122 v4; secrets are hex-encoded.
package idgen

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// NewUUIDv4 returns a random RFC 4122 version-4 UUID string. Used for webhook
// IDs minted at ingest and for request IDs.
func NewUUIDv4() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("idgen: read random: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10x
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// MustUUIDv4 is NewUUIDv4 that panics on error. Suitable for request-ID
// generation where a crypto/rand failure is fatal anyway.
func MustUUIDv4() string {
	id, err := NewUUIDv4()
	if err != nil {
		panic(err)
	}
	return id
}

// NewSecretKey returns a 256-bit random secret as a 64-char hex string, used as
// a subscription's HMAC signing secret.
func NewSecretKey() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("idgen: read random: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
