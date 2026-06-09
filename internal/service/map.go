package service

func MapStartSagaSteps[T any](steps []T, mapper func(T) StartSagaStep) []StartSagaStep {
	mapped := make([]StartSagaStep, 0, len(steps))
	for _, step := range steps {
		mapped = append(mapped, mapper(step))
	}

	return mapped
}
