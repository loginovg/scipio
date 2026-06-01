package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldReturnContextMapWhenRawContextIsJSONObject(t *testing.T) {
	t.Parallel()

	// given
	rawContext := []byte(`{"amount":42,"currency":"USD"}`)

	// when
	parsed, err := parseContext(rawContext)

	// then
	require.NoError(t, err)
	require.Equal(t, map[string]any{"amount": float64(42), "currency": "USD"}, parsed)
}

func TestShouldReturnErrInvalidSagaContextWhenRawContextIsNullLiteral(t *testing.T) {
	t.Parallel()

	// given
	rawContext := []byte(`null`)

	// when
	parsed, err := parseContext(rawContext)

	// then
	require.Nil(t, parsed)
	require.ErrorIs(t, err, ErrInvalidSagaContext)
}

func TestShouldReturnErrInvalidSagaContextWhenRawContextIsEmpty(t *testing.T) {
	t.Parallel()

	// given
	rawContext := []byte{}

	// when
	parsed, err := parseContext(rawContext)

	// then
	require.Nil(t, parsed)
	require.ErrorIs(t, err, ErrInvalidSagaContext)
}
