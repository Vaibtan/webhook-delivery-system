package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateTargetURLRejectsStorageOverflow(t *testing.T) {
	err := ValidateTargetURL("https://example.com/"+strings.Repeat("a", MaxTargetURLLength), false)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidInput)
}

func TestValidateEventType(t *testing.T) {
	require.NoError(t, ValidateEventType(strings.Repeat("a", MaxEventTypeLength)))
	require.NoError(t, ValidateEventType(strings.Repeat("界", MaxEventTypeLength)))
	err := ValidateEventType(strings.Repeat("a", MaxEventTypeLength+1))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidInput)
}

func TestValidateTargetURLCountsCharacters(t *testing.T) {
	url := "https://example.com/" + strings.Repeat("界", MaxTargetURLLength-len("https://example.com/"))
	require.NoError(t, ValidateTargetURL(url, false))
}

func TestValidUUID(t *testing.T) {
	assert.True(t, ValidUUID("123e4567-e89b-12d3-a456-426614174000"))
	assert.False(t, ValidUUID("not-a-uuid"))
	assert.False(t, ValidUUID("123e4567-e89b-12d3-a456-42661417400z"))
}
