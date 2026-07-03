package signature

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSignVerifyRoundTrip(t *testing.T) {
	msg := BuildMessage("1700000000", "", []byte(`{"a":1}`))
	sig := Sign(msg, "topsecret")
	assert.True(t, strings.HasPrefix(sig, "sha256="))
	assert.True(t, Verify(msg, "topsecret", sig))
}

func TestVerifyRejectsTampering(t *testing.T) {
	msg := BuildMessage("1700000000", "key1", []byte(`{"a":1}`))
	sig := Sign(msg, "topsecret")

	assert.False(t, Verify(msg, "wrongsecret", sig), "wrong secret must fail")
	assert.False(t, Verify(BuildMessage("1700000000", "key1", []byte(`{"a":2}`)), "topsecret", sig), "mutated body must fail")
	assert.False(t, Verify(BuildMessage("1700000001", "key1", []byte(`{"a":1}`)), "topsecret", sig), "mutated timestamp must fail")
	assert.False(t, Verify(BuildMessage("1700000000", "key2", []byte(`{"a":1}`)), "topsecret", sig), "mutated idempotency key must fail")
	assert.False(t, Verify(msg, "topsecret", "sha256=deadbeef"), "garbage signature must fail")
}

// The idempotency key is folded into signed bytes: mutating ONLY the key (same
// body+timestamp) must invalidate the signature. This is the dedupe-tamper guard.
func TestIdempotencyKeyIsSigned(t *testing.T) {
	body := []byte(`{"event":"x"}`)
	sigWithKey := Sign(BuildMessage("1700000000", "abc123", body), "s")
	assert.False(t, Verify(BuildMessage("1700000000", "", body), "s", sigWithKey))
	assert.False(t, Verify(BuildMessage("1700000000", "abc124", body), "s", sigWithKey))
}

func TestValidIdempotencyKey(t *testing.T) {
	assert.True(t, ValidIdempotencyKey(""))
	assert.True(t, ValidIdempotencyKey("abcXYZ_-019"))
	assert.True(t, ValidIdempotencyKey(strings.Repeat("a", 128)))
	assert.False(t, ValidIdempotencyKey(strings.Repeat("a", 129)))
	assert.False(t, ValidIdempotencyKey("has.dot"), "must exclude the '.' delimiter")
	assert.False(t, ValidIdempotencyKey("has space"))
	assert.False(t, ValidIdempotencyKey("emoji😀"))
}

func TestCanonicalJSONSortsKeys(t *testing.T) {
	out, err := CanonicalJSON([]byte(`{"b":1,"a":2,"c":{"z":1,"y":2}}`))
	require.NoError(t, err)
	assert.JSONEq(t, `{"a":2,"b":1,"c":{"y":2,"z":1}}`, string(out))
	// Sorted output is byte-stable:
	assert.Equal(t, `{"a":2,"b":1,"c":{"y":2,"z":1}}`, string(out))
}

func TestCanonicalJSONPreservesLargeNumbers(t *testing.T) {
	// A 64-bit int that float64 would round.
	out, err := CanonicalJSON([]byte(`{"n":9007199254740993}`))
	require.NoError(t, err)
	assert.Equal(t, `{"n":9007199254740993}`, string(out))
}

func TestCanonicalJSONRejectsTrailingTokens(t *testing.T) {
	cases := []string{
		`{}{}`,
		`{"a":1} junk`,
		`{"a":1}{"b":2}`,
		`[1,2,3]4`,
		`"str" "str2"`,
	}
	for _, c := range cases {
		_, err := CanonicalJSON([]byte(c))
		assert.Error(t, err, "must reject trailing data: %q", c)
	}
}

func TestCanonicalJSONAcceptsSingleValueWithWhitespace(t *testing.T) {
	out, err := CanonicalJSON([]byte("  {\"a\":1}\n\t "))
	require.NoError(t, err)
	assert.Equal(t, `{"a":1}`, string(out))
}
