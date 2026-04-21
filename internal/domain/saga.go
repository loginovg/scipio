package domain

import "time"

type SagaStatus string

type SagaStepStatus string

const (
	SagaStatusCreated     SagaStatus = "CREATED"
	SagaStatusRunning     SagaStatus = "RUNNING"
	SagaStatusCompleted   SagaStatus = "COMPLETED"
	SagaStatusCanceling   SagaStatus = "CANCELING"
	SagaStatusCompensated SagaStatus = "COMPENSATED"
	SagaStatusFailed      SagaStatus = "FAILED"
)

const (
	SagaStepStatusPending      SagaStepStatus = "PENDING"
	SagaStepStatusRunning      SagaStepStatus = "RUNNING"
	SagaStepStatusCompleted    SagaStepStatus = "COMPLETED"
	SagaStepStatusCompensating SagaStepStatus = "COMPENSATING"
	SagaStepStatusCompensated  SagaStepStatus = "COMPENSATED"
	SagaStepStatusFailed       SagaStepStatus = "FAILED"
)

type SagaStep struct {
	Name       string
	Status     SagaStepStatus
	Attempt    uint32
	StartedAt  *time.Time
	FinishedAt *time.Time
	Error      string
}

type ClaimedSagaStep struct {
	SagaID    string
	StepIndex int
	Name      string
	Attempt   uint32
}

type Saga struct {
	ID        string
	Workflow  string
	Status    SagaStatus
	Context   map[string]any
	Steps     []SagaStep
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (s Saga) Clone() Saga {
	copied := Saga{
		ID:        s.ID,
		Workflow:  s.Workflow,
		Status:    s.Status,
		Context:   cloneMap(s.Context),
		Steps:     cloneSteps(s.Steps),
		CreatedAt: s.CreatedAt,
		UpdatedAt: s.UpdatedAt,
	}

	return copied
}

func cloneMap(source map[string]any) map[string]any {
	if source == nil {
		return map[string]any{}
	}

	cloned := make(map[string]any, len(source))
	for key, value := range source {
		cloned[key] = cloneValue(value)
	}

	return cloned
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneMap(typed)
	case []any:
		copied := make([]any, len(typed))
		for index, item := range typed {
			copied[index] = cloneValue(item)
		}
		return copied
	default:
		return typed
	}
}

func cloneSteps(source []SagaStep) []SagaStep {
	if len(source) == 0 {
		return nil
	}

	copied := make([]SagaStep, len(source))
	for index, step := range source {
		copied[index] = SagaStep{
			Name:       step.Name,
			Status:     step.Status,
			Attempt:    step.Attempt,
			StartedAt:  copyTime(step.StartedAt),
			FinishedAt: copyTime(step.FinishedAt),
			Error:      step.Error,
		}
	}

	return copied
}

func copyTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}

	copied := value.UTC()
	return &copied
}
