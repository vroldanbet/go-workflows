package postgres

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/internal/contextpropagation"
	"github.com/cschleiden/go-workflows/internal/converter"
	"github.com/cschleiden/go-workflows/internal/core"
	"github.com/cschleiden/go-workflows/internal/history"
	"github.com/cschleiden/go-workflows/internal/metrickeys"
	"github.com/cschleiden/go-workflows/internal/task"
	"github.com/cschleiden/go-workflows/metrics"
	"github.com/cschleiden/go-workflows/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/trace"
)

//go:embed schema.sql
var schema string

type postgresBackend struct {
	pool       *pgxpool.Pool
	workerName string
	options    backend.Options
}

func NewPostgresBackend(ctx context.Context, dsn string, opts ...backend.BackendOption) (*postgresBackend, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("unable to create pgx connection pool: %w", err)
	}

	if _, err := pool.Exec(ctx, schema); err != nil {
		return nil, fmt.Errorf("failed to create schema: %w", err)
	}

	return &postgresBackend{
		pool:       pool,
		workerName: fmt.Sprintf("worker-%v", uuid.NewString()),
		options:    backend.ApplyOptions(opts...),
	}, nil
}

func (pb *postgresBackend) CreateWorkflowInstance(ctx context.Context, instance *workflow.Instance, event *history.Event) error {
	return pb.Tx(ctx, func(tx pgx.Tx) error {
		// Create workflow instance
		if err := createInstance(ctx, tx, instance, event.Attributes.(*history.ExecutionStartedAttributes).Metadata, false); err != nil {
			return err
		}

		// Initial history is empty, store only new events
		if err := insertPendingEvents(ctx, tx, instance, []*history.Event{event}); err != nil {
			return fmt.Errorf("inserting new event: %w", err)
		}

		return nil
	})
}

func (pb *postgresBackend) CancelWorkflowInstance(ctx context.Context, instance *workflow.Instance, cancelEvent *history.Event) error {
	//TODO implement me
	panic("implement me")
}

func (pb *postgresBackend) RemoveWorkflowInstance(ctx context.Context, instance *workflow.Instance) error {
	return pb.Tx(ctx, func(tx pgx.Tx) error {
		state, err := pb.getInstanceState(ctx, tx, instance)
		if err != nil {
			return err
		}

		if state == core.WorkflowInstanceStateActive {
			return backend.ErrInstanceNotFinished
		}

		if err := pb.deleteInstanceAndHistory(ctx, tx, instance); err != nil {
			return err
		}

		return nil
	})
}

func (pb *postgresBackend) GetWorkflowInstanceState(ctx context.Context, instance *workflow.Instance) (core.WorkflowInstanceState, error) {
	//TODO implement me
	panic("implement me")
}

func (pb *postgresBackend) GetWorkflowInstanceHistory(ctx context.Context, instance *workflow.Instance, lastSequenceID *int64) ([]*history.Event, error) {
	var h []*history.Event
	err := pb.Tx(ctx, func(tx pgx.Tx) error {
		var err error
		var historyEvents pgx.Rows
		if lastSequenceID != nil {
			historyEvents, err = tx.Query(
				ctx,
				"SELECT event_id, sequence_id, event_type, timestamp, schedule_event_id, attributes, visible_at FROM history WHERE instance_id = $1 AND execution_id = $2 AND sequence_id > $3 ORDER BY sequence_id",
				instance.InstanceID,
				instance.ExecutionID,
				*lastSequenceID,
			)
		} else {
			historyEvents, err = tx.Query(
				ctx,
				"SELECT event_id, sequence_id, event_type, timestamp, schedule_event_id, attributes, visible_at FROM history WHERE instance_id = $1 AND execution_id = $2 ORDER BY sequence_id",
				instance.InstanceID,
				instance.ExecutionID,
			)
		}
		if err != nil {
			return fmt.Errorf("getting history: %w", err)
		}

		for historyEvents.Next() {
			historyEvent, err := pb.deserializeEvent(historyEvents)
			if err != nil {
				return fmt.Errorf("deserializing history event: %w", err)
			}

			h = append(h, historyEvent)
		}

		return nil
	})

	return h, err
}

func (pb *postgresBackend) deserializeEvent(row pgx.Rows) (*history.Event, error) {
	var attributes []byte
	historyEvent := &history.Event{}
	if err := row.Scan(
		&historyEvent.ID,
		&historyEvent.SequenceID,
		&historyEvent.Type,
		&historyEvent.Timestamp,
		&historyEvent.ScheduleEventID,
		&attributes,
		&historyEvent.VisibleAt,
	); err != nil {
		return nil, fmt.Errorf("scanning event: %w", err)
	}

	a, err := history.DeserializeAttributes(historyEvent.Type, attributes)
	if err != nil {
		return nil, fmt.Errorf("deserializing attributes: %w", err)
	}

	historyEvent.Attributes = a
	return historyEvent, nil
}

func (pb *postgresBackend) SignalWorkflow(ctx context.Context, instanceID string, event *history.Event) error {
	//TODO implement me
	panic("implement me")
}

func (pb *postgresBackend) GetWorkflowTask(ctx context.Context) (*task.Workflow, error) {
	var wt *task.Workflow

	err := pb.Tx(ctx, func(tx pgx.Tx) error {
		id, w, now, err := pb.queryWorkflowWithPendingEvents(ctx, tx)
		if err != nil {
			return err
		}

		// workflow was not found
		if w == nil {
			return nil
		}

		if err := pb.lockWorkflow(ctx, tx, now, id); err != nil {
			return err
		}

		eventsHydrated, err := hydratePendingEvents(ctx, tx, w, now)
		if err != nil {
			return err
		}

		if !eventsHydrated {
			return nil
		}

		err = hydrateSequenceID(ctx, tx, w)
		if err != nil {
			return err
		}

		wt = w
		return nil
	})

	return wt, err
}

func (pb *postgresBackend) ExtendWorkflowTask(ctx context.Context, taskID string, instance *core.WorkflowInstance) error {
	//TODO implement me
	panic("implement me")
}

func (pb *postgresBackend) CompleteWorkflowTask(ctx context.Context, task *task.Workflow, instance *workflow.Instance, state core.WorkflowInstanceState, executedEvents, activityEvents, timerEvents []*history.Event, workflowEvents []history.WorkflowEvent) error {
	return pb.Tx(ctx, func(tx pgx.Tx) error {
		if err := pb.unlockWorkflowInstance(ctx, tx, instance, state); err != nil {
			return fmt.Errorf("unlocking workflow instance: %w", err)
		}

		if err := deleteHandledNewEvents(ctx, tx, instance, executedEvents); err != nil {
			return fmt.Errorf("deleting handled events: %w", err)
		}

		if err := insertHistoryEvents(ctx, tx, instance, executedEvents); err != nil {
			return fmt.Errorf("inserting new history events: %w", err)
		}

		if err := scheduleActivity(ctx, tx, instance, activityEvents); err != nil {
			return fmt.Errorf("scheduling activity: %w", err)
		}

		if err := insertPendingEvents(ctx, tx, instance, timerEvents); err != nil {
			return fmt.Errorf("scheduling timers: %w", err)
		}

		if err := deleteFutureEvents(ctx, tx, instance, timerEvents); err != nil {
			return fmt.Errorf("scheduling timers: %w", err)
		}

		if err := pb.insertNewWorkflowEvents(ctx, tx, workflowEvents); err != nil {
			return fmt.Errorf("new workflow events: %w", err)
		}

		return nil
	})
}

func (pb *postgresBackend) GetActivityTask(ctx context.Context) (*task.Activity, error) {
	//TODO implement me
	panic("implement me")
}

func (pb *postgresBackend) CompleteActivityTask(ctx context.Context, instance *workflow.Instance, activityID string, event *history.Event) error {
	//TODO implement me
	panic("implement me")
}

func (pb *postgresBackend) ExtendActivityTask(ctx context.Context, activityID string) error {
	//TODO implement me
	panic("implement me")
}

func (pb *postgresBackend) GetStats(ctx context.Context) (*backend.Stats, error) {
	//TODO implement me
	panic("implement me")
}

func (pb *postgresBackend) Logger() *slog.Logger {
	return pb.options.Logger
}

func (pb *postgresBackend) Tracer() trace.Tracer {
	return pb.options.TracerProvider.Tracer(backend.TracerName)
}

func (pb *postgresBackend) Metrics() metrics.Client {
	return pb.options.Metrics.WithTags(metrics.Tags{metrickeys.Backend: "mysql"})
}

func (pb *postgresBackend) Converter() converter.Converter {
	return pb.options.Converter
}

func (pb *postgresBackend) ContextPropagators() []contextpropagation.ContextPropagator {
	return pb.options.ContextPropagators
}
