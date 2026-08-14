package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ParseSagaContext_ReturnContextMapWhenSagaContextIsValidJSONObject(t *testing.T) {
	t.Parallel()

	// given
	rawContext := []byte(`{"amount":42,"currency":"USD"}`)

	// when
	parsed, err := ParseSagaContext(rawContext)

	// then
	require.NoError(t, err)
	require.Equal(t, map[string]any{"amount": float64(42), "currency": "USD"}, parsed)
}

func Test_ParseSagaContext_ReturnErrInvalidSagaContextWhenSagaContextIsEmpty(t *testing.T) {
	t.Parallel()

	// given
	rawContext := []byte{}

	// when
	parsed, err := ParseSagaContext(rawContext)

	// then
	require.Nil(t, parsed)
	require.ErrorIs(t, err, ErrInvalidSagaContext)
}

func Test_ParseSagaContext_ReturnErrInvalidSagaContextWhenSagaContextIsNullLiteral(t *testing.T) {
	t.Parallel()

	// given
	rawContext := []byte(`null`)

	// when
	parsed, err := ParseSagaContext(rawContext)

	// then
	require.Nil(t, parsed)
	require.ErrorIs(t, err, ErrInvalidSagaContext)
}

func Test_ParseSagaContext_ReturnErrInvalidSagaContextWhenSagaContextIsInvalidJSON(t *testing.T) {
	t.Parallel()

	// given
	rawContext := []byte(`{"payload":`)

	// when
	parsed, err := ParseSagaContext(rawContext)

	// then
	require.Nil(t, parsed)
	require.ErrorIs(t, err, ErrInvalidSagaContext)
}

func Test_ParseSagaContext_ReturnErrInvalidSagaContextWhenSagaContextExceedsSizeLimit(t *testing.T) {
	t.Parallel()

	// given
	rawContext := []byte(`{"payload":"` + strings.Repeat("a", MaxSagaContextBytes) + `"}`)

	// when
	parsed, err := ParseSagaContext(rawContext)

	// then
	require.Nil(t, parsed)
	require.ErrorIs(t, err, ErrInvalidSagaContext)
}
