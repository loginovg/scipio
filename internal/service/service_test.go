package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"scipio/internal/domain"
	"scipio/internal/lock"
	"scipio/internal/store"

	"github.com/stretchr/testify/require"
)

func TestShouldReturnSagaIDWhenSagaStartsWithValidInput(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService(t)

	// when
	sagaID, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{"amount": 42}`), startSteps("order_flow"))

	// then
	require.NoError(t, err)
	require.NotEmpty(t, sagaID)

	require.Eventually(t, func() bool {
		saga, getErr := svc.GetSaga(context.Background(), sagaID)
		if getErr != nil {
			return false
		}

		return saga.Status == domain.SagaStatusCreated &&
			len(saga.Steps) == 1 &&
			saga.Steps[0].Status == domain.SagaStepStatusPending
	}, 2*time.Second, 20*time.Millisecond)
}

func TestShouldCreatePendingStepWhenSagaStarts(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService(t)

	// when
	sagaID, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{"amount": 42}`), startSteps("order_flow"))

	// then
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		saga, getErr := svc.GetSaga(context.Background(), sagaID)
		if getErr != nil {
			return false
		}

		if saga.Status != domain.SagaStatusCreated {
			return false
		}

		if len(saga.Steps) != 1 {
			return false
		}

		step := saga.Steps[0]
		return step.Name == "order_flow" && step.Status == domain.SagaStepStatusPending
	}, 2*time.Second, 20*time.Millisecond)
}

func TestShouldCreateConfiguredStepWhenSagaStartsWithSteps(t *testing.T) {
	t.Parallel()

	svc := newTestService(t)

	sagaID, err := svc.StartSaga(
		context.Background(),
		"order_flow",
		[]byte(`{"amount": 42}`),
		[]StartSagaStep{{Name: "charge", GRPCTarget: "127.0.0.1:9100"}},
	)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		saga, getErr := svc.GetSaga(context.Background(), sagaID)
		if getErr != nil {
			return false
		}

		if saga.Status != domain.SagaStatusCreated || len(saga.Steps) != 1 {
			return false
		}

		step := saga.Steps[0]
		return step.Name == "charge" &&
			step.GRPCTarget == "127.0.0.1:9100" &&
			step.Status == domain.SagaStepStatusPending
	}, 2*time.Second, 20*time.Millisecond)
}

func TestShouldReturnSameSagaIDWhenStartSagaRetriedWithSameIdempotencyKey(t *testing.T) {
	t.Parallel()

	svc := newTestService(t)

	firstID, firstErr := svc.StartSagaWithIdempotencyKey(
		context.Background(),
		"order_flow",
		"retry-key-1",
		[]byte(`{"amount": 42}`),
		startSteps("order_flow"),
	)
	require.NoError(t, firstErr)

	secondID, secondErr := svc.StartSagaWithIdempotencyKey(
		context.Background(),
		"order_flow",
		"retry-key-1",
		[]byte(`{"amount": 42}`),
		startSteps("order_flow"),
	)
	require.NoError(t, secondErr)

	require.Equal(t, firstID, secondID)

	sagas, listErr := svc.ListSagas(context.Background(), "", 50, 0)
	require.NoError(t, listErr)
	require.Len(t, sagas, 1)
}

func TestShouldReturnErrInvalidWorkflowWhenWorkflowIsBlank(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService(t)

	// when
	_, err := svc.StartSaga(context.Background(), "", []byte(`{}`), nil)

	// then
	require.ErrorIs(t, err, ErrInvalidWorkflow)
}

func TestShouldReturnErrInvalidContextWhenContextIsInvalidJSON(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService(t)

	// when
	_, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{"amount":`), nil)

	// then
	require.ErrorIs(t, err, ErrInvalidContext)
}

func TestShouldReturnErrInvalidContextWhenContextExceedsSizeLimit(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService(t)
	rawContext := []byte(`{"payload":"` + strings.Repeat("a", domain.MaxSagaContextBytes) + `"}`)

	// when
	_, err := svc.StartSaga(context.Background(), "order_flow", rawContext, startSteps("order_flow"))

	// then
	require.ErrorIs(t, err, ErrInvalidContext)
}

func TestShouldReturnErrStepsRequiredWhenStepsAreMissing(t *testing.T) {
	t.Parallel()

	svc := newTestService(t)

	_, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{}`), nil)

	require.ErrorIs(t, err, ErrStepsRequired)
}

func TestShouldReturnErrInvalidStepNameWhenStepNameIsBlank(t *testing.T) {
	t.Parallel()

	svc := newTestService(t)

	_, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{}`), []StartSagaStep{{Name: "", GRPCTarget: "127.0.0.1:9000"}})

	require.ErrorIs(t, err, ErrInvalidStepName)
}

func TestShouldReturnErrInvalidStepGRPCTargetWhenStepTargetIsBlank(t *testing.T) {
	t.Parallel()

	svc := newTestService(t)

	_, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{}`), []StartSagaStep{{Name: "charge", GRPCTarget: ""}})

	require.ErrorIs(t, err, ErrInvalidStepGRPCTarget)
}

func TestShouldReturnErrCompensationNotImplementedWhenCancelRequested(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService(t)

	sagaID, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{}`), startSteps("order_flow"))
	require.NoError(t, err)

	// when
	saga, cancelErr := svc.CancelSaga(context.Background(), sagaID)

	// then
	require.ErrorIs(t, cancelErr, ErrCompensationNotImplemented)
	require.Equal(t, domain.Saga{}, saga)

	storedSaga, getErr := svc.GetSaga(context.Background(), sagaID)
	require.NoError(t, getErr)
	require.Equal(t, domain.SagaStatusCanceling, storedSaga.Status)
	require.Len(t, storedSaga.Steps, 1)
	require.Equal(t, domain.SagaStepStatusPending, storedSaga.Steps[0].Status)
}

func TestShouldReturnErrCompensationNotImplementedWhenCancelRequestedForRunningSaga(t *testing.T) {
	t.Parallel()

	memoryStore := store.NewMemory()
	svc, newErr := New(memoryStore, lock.NewNoop(), time.Second)
	require.NoError(t, newErr)

	now := time.Now().UTC()
	createErr := memoryStore.Create(context.Background(), domain.Saga{
		ID:       "running-saga",
		Workflow: "order_flow",
		Status:   domain.SagaStatusRunning,
		Context:  map[string]any{"ref": "running"},
		Steps: []domain.SagaStep{
			{
				Name:       "order_flow",
				GRPCTarget: "127.0.0.1:9100",
				Status:     domain.SagaStepStatusRunning,
				Attempt:    1,
				StartedAt:  &now,
			},
		},
		CreatedAt: now.Add(-time.Minute),
		UpdatedAt: now.Add(-time.Minute),
	})
	require.NoError(t, createErr)

	saga, cancelErr := svc.CancelSaga(context.Background(), "running-saga")

	require.ErrorIs(t, cancelErr, ErrCompensationNotImplemented)
	require.Equal(t, domain.Saga{}, saga)

	storedSaga, getErr := memoryStore.Get(context.Background(), "running-saga")
	require.NoError(t, getErr)
	require.Equal(t, domain.SagaStatusCanceling, storedSaga.Status)
	require.Len(t, storedSaga.Steps, 1)
	require.Equal(t, domain.SagaStepStatusRunning, storedSaga.Steps[0].Status)
	require.Equal(t, uint32(1), storedSaga.Steps[0].Attempt)
	require.NotNil(t, storedSaga.Steps[0].StartedAt)
}

func TestShouldReturnErrSagaCancelNotAllowedWhenCancelRequestedForFailedSaga(t *testing.T) {
	t.Parallel()

	memoryStore := store.NewMemory()
	svc, newErr := New(memoryStore, lock.NewNoop(), time.Second)
	require.NoError(t, newErr)

	now := time.Now().UTC()
	createErr := memoryStore.Create(context.Background(), domain.Saga{
		ID:       "failed-saga",
		Workflow: "order_flow",
		Status:   domain.SagaStatusFailed,
		Context:  map[string]any{"ref": "failed"},
		Steps: []domain.SagaStep{
			{
				Name:       "order_flow",
				GRPCTarget: "127.0.0.1:9100",
				Status:     domain.SagaStepStatusFailed,
				Attempt:    1,
				Error:      "step failed",
			},
		},
		CreatedAt: now.Add(-time.Minute),
		UpdatedAt: now.Add(-time.Minute),
	})
	require.NoError(t, createErr)

	saga, cancelErr := svc.CancelSaga(context.Background(), "failed-saga")

	require.ErrorIs(t, cancelErr, ErrSagaCancelNotAllowed)
	require.Equal(t, domain.Saga{}, saga)

	storedSaga, getErr := memoryStore.Get(context.Background(), "failed-saga")
	require.NoError(t, getErr)
	require.Equal(t, domain.SagaStatusFailed, storedSaga.Status)
	require.Len(t, storedSaga.Steps, 1)
	require.Equal(t, domain.SagaStepStatusFailed, storedSaga.Steps[0].Status)
	require.Equal(t, "step failed", storedSaga.Steps[0].Error)
}

func TestShouldFilterSagasByStatusWhenStatusFilterProvided(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService(t)

	firstID, firstErr := svc.StartSaga(context.Background(), "order_flow", []byte(`{"idx": 1}`), startSteps("order_flow"))
	require.NoError(t, firstErr)

	secondID, secondErr := svc.StartSaga(context.Background(), "order_flow", []byte(`{"idx": 2}`), startSteps("order_flow"))
	require.NoError(t, secondErr)

	// when
	_, cancelErr := svc.CancelSaga(context.Background(), secondID)
	require.ErrorIs(t, cancelErr, ErrCompensationNotImplemented)

	require.Eventually(t, func() bool {
		sagas, listErr := svc.ListSagas(context.Background(), string(domain.SagaStatusCanceling), 50, 0)
		if listErr != nil {
			return false
		}

		if len(sagas) != 1 {
			return false
		}

		return sagas[0].ID == secondID
	}, 2*time.Second, 20*time.Millisecond)

	// then
	sagas, listErr := svc.ListSagas(context.Background(), "", 50, 0)
	require.NoError(t, listErr)
	require.Len(t, sagas, 2)
	require.NotEqual(t, firstID, secondID)
}

func TestShouldReturnErrInvalidStatusFilterWhenFilterIsUnknown(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService(t)

	// when
	_, err := svc.ListSagas(context.Background(), "UNKNOWN", 50, 0)

	// then
	require.ErrorIs(t, err, ErrInvalidStatusFilter)
}

func TestShouldListSagasWhenStatusFilterContainsOnlyWhitespace(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService(t)

	_, firstErr := svc.StartSaga(context.Background(), "order_flow", []byte(`{"idx": 1}`), startSteps("order_flow"))
	require.NoError(t, firstErr)

	_, secondErr := svc.StartSaga(context.Background(), "order_flow", []byte(`{"idx": 2}`), startSteps("order_flow"))
	require.NoError(t, secondErr)

	// when
	sagas, listErr := svc.ListSagas(context.Background(), "   \t", 50, 0)

	// then
	require.NoError(t, listErr)
	require.Len(t, sagas, 2)
}

func TestShouldReturnErrInvalidPaginationWhenPaginationIsNegative(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService(t)

	// when
	_, err := svc.ListSagas(context.Background(), "", -1, 0)

	// then
	require.ErrorIs(t, err, ErrInvalidPagination)

	// when
	_, err = svc.ListSagas(context.Background(), "", 10, -1)

	// then
	require.ErrorIs(t, err, ErrInvalidPagination)
}

func TestShouldReturnErrNotFoundWhenSagaDoesNotExist(t *testing.T) {
	t.Parallel()

	// given
	svc := newTestService(t)

	// when
	_, err := svc.GetSaga(context.Background(), "missing")

	// then
	require.ErrorIs(t, err, ErrSagaNotFound)
}

func TestShouldReturnLockErrorWhenSagaLockAcquisitionFailsDuringCancel(t *testing.T) {
	t.Parallel()

	// given
	lockErr := errors.New("lock unavailable")
	svc, newErr := New(store.NewMemory(), failingLocker{err: lockErr}, time.Second)
	require.NoError(t, newErr)

	sagaID, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{}`), startSteps("order_flow"))
	require.NoError(t, err)

	// when
	_, cancelErr := svc.CancelSaga(context.Background(), sagaID)

	// then
	require.ErrorIs(t, cancelErr, lockErr)
}

func TestShouldReturnErrSagaLockContendedWhenSagaLockAcquisitionTimesOutDuringCancel(t *testing.T) {
	t.Parallel()

	// given
	svc, newErr := New(store.NewMemory(), failingLocker{err: lock.ErrLockContended}, time.Second)
	require.NoError(t, newErr)

	sagaID, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{}`), startSteps("order_flow"))
	require.NoError(t, err)

	// when
	_, cancelErr := svc.CancelSaga(context.Background(), sagaID)

	// then
	require.ErrorIs(t, cancelErr, ErrSagaLockContended)
}

func TestShouldReturnCancelUpdateErrorWhenCancelUpdateFailsDuringCancel(t *testing.T) {
	t.Parallel()

	// given
	cancelUpdateErr := errors.New("cancel update failed")
	storeWithFailedCancelUpdate := &failingUpdateStore{
		inner:        store.NewMemory(),
		failAtUpdate: 1,
		err:          cancelUpdateErr,
	}
	svc, newErr := New(storeWithFailedCancelUpdate, lock.NewNoop(), time.Second)
	require.NoError(t, newErr)

	sagaID, err := svc.StartSaga(context.Background(), "order_flow", []byte(`{}`), startSteps("order_flow"))
	require.NoError(t, err)

	// when
	_, cancelErr := svc.CancelSaga(context.Background(), sagaID)

	// then
	require.ErrorIs(t, cancelErr, cancelUpdateErr)
}

func TestShouldReturnErrStoreNotConfiguredWhenStoreIsNil(t *testing.T) {
	t.Parallel()

	// given
	svc, newErr := New(nil, lock.NewNoop(), time.Second)
	require.NoError(t, newErr)

	// when
	_, startErr := svc.StartSaga(context.Background(), "order_flow", []byte(`{}`), nil)

	// then
	require.ErrorIs(t, startErr, ErrStoreNotConfigured)

	// when
	_, getErr := svc.GetSaga(context.Background(), "saga-id")

	// then
	require.ErrorIs(t, getErr, ErrStoreNotConfigured)

	// when
	_, cancelErr := svc.CancelSaga(context.Background(), "saga-id")

	// then
	require.ErrorIs(t, cancelErr, ErrStoreNotConfigured)

	// when
	_, listErr := svc.ListSagas(context.Background(), "", 50, 0)

	// then
	require.ErrorIs(t, listErr, ErrStoreNotConfigured)
}

func TestShouldReturnErrInvalidTTLWhenLockTTLIsNotPositiveInServiceConstructor(t *testing.T) {
	t.Parallel()

	_, err := New(store.NewMemory(), lock.NewNoop(), 0)

	require.ErrorIs(t, err, lock.ErrInvalidTTL)
}

func newTestService(t *testing.T) *Service {
	t.Helper()

	svc, err := New(store.NewMemory(), lock.NewNoop(), 5*time.Second)
	require.NoError(t, err)
	return svc
}

func startSteps(name string) []StartSagaStep {
	return []StartSagaStep{
		{
			Name:       name,
			GRPCTarget: "127.0.0.1:9100",
		},
	}
}

type failingLocker struct {
	err error
}

func (l failingLocker) Acquire(_ context.Context, _ string, _ time.Duration) (lock.Handle, error) {
	return nil, l.err
}

type failingUpdateStore struct {
	inner        *store.Memory
	failAtUpdate int
	err          error
	updates      int
}

func (s *failingUpdateStore) Create(ctx context.Context, saga domain.Saga) error {
	return s.inner.Create(ctx, saga)
}

func (s *failingUpdateStore) Get(ctx context.Context, id string) (domain.Saga, error) {
	return s.inner.Get(ctx, id)
}

func (s *failingUpdateStore) List(ctx context.Context, status *domain.SagaStatus, limit int, offset int) ([]domain.Saga, error) {
	return s.inner.List(ctx, status, limit, offset)
}

func (s *failingUpdateStore) Update(ctx context.Context, id string, fn func(*domain.Saga) error) (domain.Saga, error) {
	s.updates++
	if s.updates == s.failAtUpdate {
		return domain.Saga{}, s.err
	}

	return s.inner.Update(ctx, id, fn)
}
