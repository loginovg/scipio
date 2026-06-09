package sqlschema

import _ "embed"

//go:embed schema/sagas.sql
var sagaSchema string

func SagaSchema() string {
	return sagaSchema
}
