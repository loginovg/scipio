package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"scipio/internal/domain"
	storesqlc "scipio/internal/store/sqlc"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrInvalidPostgresConnectionString = errors.New("postgres connection string must not be empty")
var ErrInvalidSagaContext = errors.New("saga context must be a non-null JSON object")

type Postgres struct {
	pool    *pgxpool.Pool
	queries *storesqlc.Queries
}

type stepReader interface {
	GetSagaSteps(ctx context.Context, sagaID string) ([]storesqlc.GetSagaStepsRow, error)
}

type stepWriter interface {
	DeleteSagaSteps(ctx context.Context, sagaID string) error
	InsertSagaStep(ctx context.Context, arg storesqlc.InsertSagaStepParams) error
}

func NewPostgres(ctx context.Context, connectionString string) (*Postgres, error) {
	trimmedConnectionString := strings.TrimSpace(connectionString)
	if trimmedConnectionString == "" {
		return nil, ErrInvalidPostgresConnectionString
	}

	pool, err := pgxpool.New(ctx, trimmedConnectionString)
	if err != nil {
		return nil, err
	}

	if pingErr := pool.Ping(ctx); pingErr != nil {
		pool.Close()
		return nil, pingErr
	}

	return &Postgres{pool: pool, queries: storesqlc.New(pool)}, nil
}

func (p *Postgres) Close() {
	if p.pool != nil {
		p.pool.Close()
	}
}

func (p *Postgres) Migrate(ctx context.Context, migrationSQL string) error {
	if strings.TrimSpace(migrationSQL) == "" {
		return nil
	}

	_, err := p.pool.Exec(ctx, migrationSQL)
	return err
}

func (p *Postgres) Create(ctx context.Context, saga domain.Saga) error {
	serializedContext, err := json.Marshal(saga.Context)
	if err != nil {
		return err
	}

	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer rollbackTx(ctx, tx)

	qtx := p.queries.WithTx(tx)
	if createErr := qtx.CreateSaga(ctx, storesqlc.CreateSagaParams{
		ID:        saga.ID,
		Workflow:  saga.Workflow,
		Status:    string(saga.Status),
		Context:   serializedContext,
		CreatedAt: toPGTime(saga.CreatedAt),
		UpdatedAt: toPGTime(saga.UpdatedAt),
	}); createErr != nil {
		if isUniqueViolation(createErr) {
			return ErrAlreadyExists
		}
		return createErr
	}

	if stepsErr := replaceSteps(ctx, qtx, saga.ID, saga.Steps); stepsErr != nil {
		return stepsErr
	}

	return tx.Commit(ctx)
}

func (p *Postgres) Get(ctx context.Context, id string) (domain.Saga, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return domain.Saga{}, err
	}
	defer rollbackTx(ctx, tx)

	qtx := p.queries.WithTx(tx)
	sagaRow, err := qtx.GetSaga(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Saga{}, ErrNotFound
		}
		return domain.Saga{}, err
	}

	saga, mapErr := mapSagaRow(sagaRow)
	if mapErr != nil {
		return domain.Saga{}, mapErr
	}

	steps, stepsErr := fetchSteps(ctx, qtx, id)
	if stepsErr != nil {
		return domain.Saga{}, stepsErr
	}

	saga.Steps = steps
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return domain.Saga{}, commitErr
	}

	return saga, nil
}

func (p *Postgres) List(ctx context.Context, status *domain.SagaStatus, limit int, offset int) ([]domain.Saga, error) {
	if limit < 0 || offset < 0 {
		return nil, errors.New("invalid pagination")
	}

	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, err
	}
	defer rollbackTx(ctx, tx)

	qtx := p.queries.WithTx(tx)

	safeLimit := int32(limit)
	safeOffset := int32(offset)

	var (
		sagaRows []storesqlc.Saga
	)
	if status == nil {
		sagaRows, err = qtx.ListSagas(ctx, storesqlc.ListSagasParams{Limit: safeLimit, Offset: safeOffset})
	} else {
		sagaRows, err = qtx.ListSagasByStatus(ctx, storesqlc.ListSagasByStatusParams{
			Status: string(*status),
			Limit:  safeLimit,
			Offset: safeOffset,
		})
	}
	if err != nil {
		return nil, err
	}

	sagas := make([]domain.Saga, 0, len(sagaRows))
	for _, sagaRow := range sagaRows {
		saga, mapErr := mapSagaRow(sagaRow)
		if mapErr != nil {
			return nil, mapErr
		}

		steps, stepsErr := fetchSteps(ctx, qtx, saga.ID)
		if stepsErr != nil {
			return nil, stepsErr
		}

		saga.Steps = steps
		sagas = append(sagas, saga)
	}

	if commitErr := tx.Commit(ctx); commitErr != nil {
		return nil, commitErr
	}

	return sagas, nil
}

func (p *Postgres) Update(ctx context.Context, id string, fn func(*domain.Saga) error) (domain.Saga, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return domain.Saga{}, err
	}
	defer rollbackTx(ctx, tx)

	qtx := p.queries.WithTx(tx)
	sagaRow, sagaErr := qtx.GetSagaForUpdate(ctx, id)
	if sagaErr != nil {
		if errors.Is(sagaErr, pgx.ErrNoRows) {
			return domain.Saga{}, ErrNotFound
		}
		return domain.Saga{}, sagaErr
	}

	saga, mapErr := mapSagaRow(sagaRow)
	if mapErr != nil {
		return domain.Saga{}, mapErr
	}

	stepRows, stepErr := qtx.GetSagaStepsForUpdate(ctx, id)
	if stepErr != nil {
		return domain.Saga{}, stepErr
	}

	saga.Steps, mapErr = mapStepRowsForUpdate(stepRows)
	if mapErr != nil {
		return domain.Saga{}, mapErr
	}

	if fnErr := fn(&saga); fnErr != nil {
		return domain.Saga{}, fnErr
	}

	serializedContext, contextErr := json.Marshal(saga.Context)
	if contextErr != nil {
		return domain.Saga{}, contextErr
	}

	saga.UpdatedAt = time.Now().UTC()
	if updateErr := qtx.UpdateSaga(ctx, storesqlc.UpdateSagaParams{
		ID:        saga.ID,
		Workflow:  saga.Workflow,
		Status:    string(saga.Status),
		Context:   serializedContext,
		UpdatedAt: toPGTime(saga.UpdatedAt),
	}); updateErr != nil {
		return domain.Saga{}, updateErr
	}

	if replaceErr := replaceSteps(ctx, qtx, saga.ID, saga.Steps); replaceErr != nil {
		return domain.Saga{}, replaceErr
	}

	if commitErr := tx.Commit(ctx); commitErr != nil {
		return domain.Saga{}, commitErr
	}

	return saga.Clone(), nil
}

func (p *Postgres) ClaimNextStep(ctx context.Context, staleAfter time.Duration) (domain.ClaimedSagaStep, bool, error) {
	staleThreshold := time.Now().UTC().Add(-staleAfter)
	claimed, err := p.queries.ClaimNextStep(ctx, toPGTime(staleThreshold))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ClaimedSagaStep{}, false, nil
		}

		return domain.ClaimedSagaStep{}, false, err
	}

	return domain.ClaimedSagaStep{
		SagaID:    claimed.SagaID,
		StepIndex: int(claimed.StepIndex),
		Name:      claimed.Name,
		Attempt:   uint32(claimed.Attempt),
	}, true, nil
}

func fetchSteps(ctx context.Context, reader stepReader, sagaID string) ([]domain.SagaStep, error) {
	stepRows, err := reader.GetSagaSteps(ctx, sagaID)
	if err != nil {
		return nil, err
	}

	return mapStepRows(stepRows)
}

func mapSagaRow(row storesqlc.Saga) (domain.Saga, error) {
	status, statusErr := domain.ParseSagaStatus(row.Status)
	if statusErr != nil {
		return domain.Saga{}, fmt.Errorf("unsupported saga status %q", row.Status)
	}

	sagaContext, contextErr := parseContext(row.Context)
	if contextErr != nil {
		return domain.Saga{}, contextErr
	}

	return domain.Saga{
		ID:        row.ID,
		Workflow:  row.Workflow,
		Status:    status,
		Context:   sagaContext,
		CreatedAt: row.CreatedAt.Time.UTC(),
		UpdatedAt: row.UpdatedAt.Time.UTC(),
	}, nil
}

func mapStepRows(rows []storesqlc.GetSagaStepsRow) ([]domain.SagaStep, error) {
	return mapRows(rows, func(row storesqlc.GetSagaStepsRow) (domain.SagaStep, error) {
		return mapStepRow(
			row.Name,
			row.GrpcTarget,
			row.Status,
			row.Attempt,
			row.StartedAt,
			row.FinishedAt,
			row.Error,
		)
	})
}

func mapStepRowsForUpdate(rows []storesqlc.GetSagaStepsForUpdateRow) ([]domain.SagaStep, error) {
	return mapRows(rows, func(row storesqlc.GetSagaStepsForUpdateRow) (domain.SagaStep, error) {
		return mapStepRow(
			row.Name,
			row.GrpcTarget,
			row.Status,
			row.Attempt,
			row.StartedAt,
			row.FinishedAt,
			row.Error,
		)
	})
}

func mapRows[T any, R any](values []T, mapper func(T) (R, error)) ([]R, error) {
	mapped := make([]R, 0, len(values))
	for _, value := range values {
		result, err := mapper(value)
		if err != nil {
			return nil, err
		}

		mapped = append(mapped, result)
	}

	return mapped, nil
}

func mapStepRow(
	name string,
	grpcTarget string,
	statusRaw string,
	attempt int32,
	startedAt pgtype.Timestamptz,
	finishedAt pgtype.Timestamptz,
	errText pgtype.Text,
) (domain.SagaStep, error) {
	status, statusErr := domain.ParseSagaStepStatus(statusRaw)
	if statusErr != nil {
		return domain.SagaStep{}, fmt.Errorf("unsupported saga step status %q", statusRaw)
	}

	return domain.SagaStep{
		Name:       name,
		GRPCTarget: grpcTarget,
		Status:     status,
		Attempt:    uint32(attempt),
		StartedAt:  toTimePtr(startedAt),
		FinishedAt: toTimePtr(finishedAt),
		Error:      toString(errText),
	}, nil
}

func replaceSteps(ctx context.Context, writer stepWriter, sagaID string, steps []domain.SagaStep) error {
	if err := writer.DeleteSagaSteps(ctx, sagaID); err != nil {
		return err
	}

	for index, step := range steps {
		if err := writer.InsertSagaStep(ctx, storesqlc.InsertSagaStepParams{
			SagaID:     sagaID,
			StepIndex:  int32(index),
			Name:       step.Name,
			GrpcTarget: step.GRPCTarget,
			Status:     string(step.Status),
			Attempt:    int32(step.Attempt),
			StartedAt:  toNullablePGTime(step.StartedAt),
			FinishedAt: toNullablePGTime(step.FinishedAt),
			Error:      toNullableText(step.Error),
		}); err != nil {
			return err
		}
	}

	return nil
}

func rollbackTx(ctx context.Context, tx pgx.Tx) {
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		slog.WarnContext(ctx, "rollback failed", "error", err)
	}
}

func parseContext(rawContext []byte) (map[string]any, error) {
	if len(rawContext) == 0 {
		return nil, ErrInvalidSagaContext
	}

	var parsed map[string]any
	if err := json.Unmarshal(rawContext, &parsed); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSagaContext, err)
	}

	if parsed == nil {
		return nil, ErrInvalidSagaContext
	}

	return parsed, nil
}

func isUniqueViolation(err error) bool {
	var pgError *pgconn.PgError
	if !errors.As(err, &pgError) {
		return false
	}

	return pgError.Code == "23505"
}

func toPGTime(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value.UTC(), Valid: true}
}

func toNullablePGTime(value *time.Time) pgtype.Timestamptz {
	if value == nil {
		return pgtype.Timestamptz{}
	}

	return pgtype.Timestamptz{Time: value.UTC(), Valid: true}
}

func toTimePtr(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}

	copied := value.Time.UTC()
	return &copied
}

func toString(value pgtype.Text) string {
	if !value.Valid {
		return ""
	}

	return value.String
}

func toNullableText(value string) pgtype.Text {
	if strings.TrimSpace(value) == "" {
		return pgtype.Text{}
	}

	return pgtype.Text{String: value, Valid: true}
}
