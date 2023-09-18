package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/internal/core"
	"github.com/cschleiden/go-workflows/internal/history"
	"github.com/cschleiden/go-workflows/internal/task"
	"github.com/cschleiden/go-workflows/workflow"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (pb *postgresBackend) Tx(ctx context.Context, f func(tx pgx.Tx) error) error {
	tx, err := pb.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := f(tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("creating workflow instance: %w", err)
	}

	return nil
}

func (pb *postgresBackend) getInstanceState(ctx context.Context, tx pgx.Tx, instance *workflow.Instance) (core.WorkflowInstanceState, error) {
	row := tx.QueryRow(ctx, "SELECT state FROM instances WHERE instance_id = $1 AND execution_id = $2 LIMIT 1", instance.InstanceID, instance.ExecutionID)
	var state core.WorkflowInstanceState
	if err := row.Scan(&state); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, backend.ErrInstanceNotFound
		}

		return 0, err
	}

	return state, nil
}

func (pb *postgresBackend) deleteInstanceAndHistory(ctx context.Context, tx pgx.Tx, instance *workflow.Instance) error {
	batch := &pgx.Batch{}
	batch.Queue("DELETE FROM `instances` WHERE instance_id = $1 AND execution_id = $2", instance.InstanceID, instance.ExecutionID)
	batch.Queue("DELETE FROM `history` WHERE instance_id = $1 AND execution_id = $2", instance.InstanceID, instance.ExecutionID)
	b := tx.SendBatch(ctx, batch)
	if _, err := b.Exec(); err != nil {
		return err
	}
	return nil
}

func hydrateSequenceID(ctx context.Context, tx pgx.Tx, w *task.Workflow) error {
	row := tx.QueryRow(ctx, `SELECT sequence_id 
								FROM history WHERE instance_id = $1 AND execution_id = $2 
								ORDER BY id DESC LIMIT 1`,
		w.WorkflowInstance.InstanceID,
		w.WorkflowInstance.ExecutionID)
	if err := row.Scan(
		&w.LastSequenceID,
	); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("getting most recent sequence id: %w", err)
		}
	}
	return nil
}

func hydratePendingEvents(ctx context.Context, tx pgx.Tx, w *task.Workflow, now time.Time) (bool, error) {
	// Get new events
	events, err := tx.Query(
		ctx,
		`SELECT event_id, sequence_id, event_type, timestamp, schedule_event_id, attributes, visible_at
				FROM pending_events
				WHERE instance_id = $1 AND execution_id = $2 AND (visible_at IS NULL OR visible_at <= $3)
				ORDER BY id`,
		w.WorkflowInstance.InstanceID,
		w.WorkflowInstance.ExecutionID,
		now,
	)
	if err != nil {
		return false, fmt.Errorf("getting new events: %w", err)
	}

	for events.Next() {
		var attributes []byte

		historyEvent := &history.Event{}

		if err := events.Scan(
			&historyEvent.ID,
			&historyEvent.SequenceID,
			&historyEvent.Type,
			&historyEvent.Timestamp,
			&historyEvent.ScheduleEventID,
			&attributes,
			&historyEvent.VisibleAt,
		); err != nil {
			return false, fmt.Errorf("scanning event: %w", err)
		}

		a, err := history.DeserializeAttributes(historyEvent.Type, attributes)
		if err != nil {
			return false, fmt.Errorf("deserializing attributes: %w", err)
		}

		historyEvent.Attributes = a

		w.NewEvents = append(w.NewEvents, historyEvent)
	}

	return len(w.NewEvents) != 0, nil
}

func (pb *postgresBackend) queryWorkflowWithPendingEvents(ctx context.Context, tx pgx.Tx) (int, *task.Workflow, time.Time, error) {
	row := tx.QueryRow(
		ctx,
		`SELECT i.id, i.instance_id, i.execution_id, i.parent_instance_id, i.parent_execution_id, i.parent_schedule_event_id, i.metadata, i.sticky_until, now()
			FROM instances i
			INNER JOIN pending_events pe ON i.instance_id = pe.instance_id
			WHERE
				i.completed_at IS NULL
				AND (pe.visible_at IS NULL OR pe.visible_at <= now())
				AND (i.locked_until IS NULL OR i.locked_until < now())
				AND (i.sticky_until IS NULL OR i.sticky_until < now() OR i.worker = $1)
			LIMIT 1
			FOR UPDATE OF i SKIP LOCKED`,
		pb.workerName, // worker
	)

	var id int
	var instanceID, executionID string
	var parentInstanceID, parentExecutionID *string
	var parentEventID *int64
	var metadataJson sql.NullString
	var stickyUntil *time.Time
	var now time.Time
	if err := row.Scan(&id, &instanceID, &executionID, &parentInstanceID, &parentExecutionID, &parentEventID, &metadataJson, &stickyUntil, &now); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil, now, nil
		}

		return 0, nil, now, fmt.Errorf("scanning workflow instance: %w", err)
	}

	var wfi *workflow.Instance
	if parentInstanceID != nil {
		wfi = core.NewSubWorkflowInstance(instanceID, executionID, core.NewWorkflowInstance(*parentInstanceID, *parentExecutionID), *parentEventID)
	} else {
		wfi = core.NewWorkflowInstance(instanceID, executionID)
	}

	var metadata *core.WorkflowMetadata
	if metadataJson.Valid {
		if err := json.Unmarshal([]byte(metadataJson.String), &metadata); err != nil {
			return 0, nil, now, fmt.Errorf("parsing workflow metadata: %w", err)
		}
	}

	t := &task.Workflow{
		ID:                    wfi.InstanceID,
		WorkflowInstance:      wfi,
		WorkflowInstanceState: core.WorkflowInstanceStateActive,
		Metadata:              metadata,
		NewEvents:             []*history.Event{},
	}

	return id, t, now, nil
}

func (pb *postgresBackend) lockWorkflow(ctx context.Context, tx pgx.Tx, now time.Time, id int) error {
	res, err := tx.Exec(
		ctx,
		`UPDATE instances i
			SET locked_until = $1, worker = $2
			WHERE id = $3`,
		now.Add(pb.options.WorkflowLockTimeout),
		pb.workerName,
		id,
	)
	if err != nil {
		return fmt.Errorf("locking workflow instance: %w", err)
	}

	if res.RowsAffected() == 0 {
		return fmt.Errorf("attempted to lock workflow but zero rows were affected")
	}

	return nil
}

func deleteHandledNewEvents(ctx context.Context, tx pgx.Tx, instance *workflow.Instance, executedEvents []*history.Event) error {
	if len(executedEvents) > 0 {
		batch := &pgx.Batch{}
		for _, event := range executedEvents {
			batch.Queue(`DELETE FROM pending_events WHERE instance_id = $1 AND execution_id = $2 AND event_id = $3`, instance.InstanceID, instance.ExecutionID, event.ID)
		}

		b := tx.SendBatch(ctx, batch)
		if _, err := b.Exec(); err != nil {
			return fmt.Errorf("deleting handled new events: %w", err)
		}

		return b.Close()
	}

	return nil
}

func deleteFutureEvents(ctx context.Context, tx pgx.Tx, instance *workflow.Instance, events []*history.Event) error {
	if len(events) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, event := range events {
		if event.Type != history.EventType_TimerCanceled {
			continue
		}

		batch.Queue(`DELETE FROM pending_events WHERE instance_id = $1 AND execution_id = $2 AND schedule_event_id = $3 AND visible_at IS NOT NULL`,
			instance.InstanceID, instance.ExecutionID, event.ScheduleEventID)
	}

	if batch.Len() == 0 {
		return nil
	}

	b := tx.SendBatch(ctx, batch)
	if _, err := b.Exec(); err != nil {
		return fmt.Errorf("deleting handled new events: %w", err)
	}

	return b.Close()
}

func (pb *postgresBackend) unlockWorkflowInstance(ctx context.Context, tx pgx.Tx, instance *workflow.Instance, state core.WorkflowInstanceState) error {
	query := `UPDATE instances SET locked_until = NULL, sticky_until = now() + $1, state = $2 WHERE instance_id = $3 AND execution_id = $4 AND worker = $5 AND locked_until IS NOT NULL`
	if state == core.WorkflowInstanceStateContinuedAsNew || state == core.WorkflowInstanceStateFinished {
		query = `UPDATE instances SET locked_until = NULL, sticky_until = now() + $1, completed_at = now(), state = $2 WHERE instance_id = $3 AND execution_id = $4 AND worker = $5 AND locked_until IS NOT NULL`
	}

	res, err := tx.Exec(
		ctx,
		query,
		pb.options.StickyTimeout,
		state,
		instance.InstanceID,
		instance.ExecutionID,
		pb.workerName,
	)
	if err != nil {
		return fmt.Errorf("unlocking instance: %w", err)
	}

	if res.RowsAffected() != 1 {
		return errors.New("could not find workflow instance to unlock")
	}
	return nil
}

func createInstance(ctx context.Context, tx pgx.Tx, wfi *workflow.Instance, metadata *workflow.Metadata, ignoreDuplicate bool) error {
	var parentInstanceID, parentExecutionID *string
	var parentEventID *int64
	if wfi.SubWorkflow() {
		parentInstanceID = &wfi.Parent.InstanceID
		parentExecutionID = &wfi.Parent.ExecutionID
		parentEventID = &wfi.ParentEventID
	}

	metadataJson, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}

	// FIXME missing "IGNORE" in SQL statement - find equivalent
	_, err = tx.Exec(
		ctx,
		"INSERT INTO instances (instance_id, execution_id, parent_instance_id, parent_execution_id, parent_schedule_event_id, metadata, state) VALUES ($1, $2, $3, $4, $5, $6, $7);",
		wfi.InstanceID,
		wfi.ExecutionID,
		parentInstanceID,
		parentExecutionID,
		parentEventID,
		string(metadataJson),
		core.WorkflowInstanceStateActive,
	)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return backend.ErrInstanceAlreadyExists
	} else if err != nil {
		panic(err)
	}
	if err != nil {
		return fmt.Errorf("inserting workflow instance: %w", err)
	}

	return nil
}

func insertPendingEvents(ctx context.Context, tx pgx.Tx, instance *core.WorkflowInstance, events []*history.Event) error {
	return insertEvents(ctx, tx, "pending_events", instance, events)
}

func insertHistoryEvents(ctx context.Context, tx pgx.Tx, instance *core.WorkflowInstance, events []*history.Event) error {
	return insertEvents(ctx, tx, "history", instance, events)
}

func insertEvents(ctx context.Context, tx pgx.Tx, tableName string, instance *core.WorkflowInstance, events []*history.Event) error {
	if len(events) == 0 {
		return nil
	}

	query := "INSERT INTO " + tableName + " (event_id, sequence_id, instance_id, execution_id, event_type, timestamp, schedule_event_id, attributes, visible_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
	const batchSize = 20
	for batchStart := 0; batchStart < len(events); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(events) {
			batchEnd = len(events)
		}
		batchEvents := events[batchStart:batchEnd]
		batch := &pgx.Batch{}

		for _, newEvent := range batchEvents {
			a, err := history.SerializeAttributes(newEvent.Attributes)
			if err != nil {
				return fmt.Errorf("serializing events: %w", err)
			}

			batch.Queue(query, newEvent.ID, newEvent.SequenceID, instance.InstanceID, instance.ExecutionID,
				newEvent.Type, newEvent.Timestamp, newEvent.ScheduleEventID, a, newEvent.VisibleAt)
		}

		b := tx.SendBatch(ctx, batch)
		if _, err := b.Exec(); err != nil {
			return fmt.Errorf("inserting events: %w", err)
		}

		if err := b.Close(); err != nil {
			return err
		}
	}

	return nil
}

func scheduleActivity(ctx context.Context, tx pgx.Tx, instance *workflow.Instance, events []*history.Event) error {
	if len(events) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, event := range events {
		attrs, err := history.SerializeAttributes(event.Attributes)
		if err != nil {
			return fmt.Errorf("serializing event attributes: %w", err)
		}

		batch.Queue(
			`INSERT INTO activities
			(activity_id, instance_id, execution_id, event_type, timestamp, schedule_event_id, attributes, visible_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			event.ID,
			instance.InstanceID,
			instance.ExecutionID,
			event.Type,
			event.Timestamp,
			event.ScheduleEventID,
			attrs,
			event.VisibleAt,
		)
	}

	b := tx.SendBatch(ctx, batch)
	if _, err := b.Exec(); err != nil {
		return fmt.Errorf("scheduling activities: %w", err)
	}

	return b.Close()
}

func (pb *postgresBackend) insertNewWorkflowEvents(ctx context.Context, tx pgx.Tx, workflowEvents []history.WorkflowEvent) error {
	eventsByWorkflowInstance := history.EventsByWorkflowInstance(workflowEvents)

	// FIXME optimize this with a batch
	for targetInstance, events := range eventsByWorkflowInstance {
		for _, m := range events {
			if m.HistoryEvent.Type == history.EventType_WorkflowExecutionStarted {
				a := m.HistoryEvent.Attributes.(*history.ExecutionStartedAttributes)

				if err := createInstance(ctx, tx, m.WorkflowInstance, a.Metadata, true); err != nil {
					return err
				}

				break
			}
		}

		historyEvents := make([]*history.Event, 0, len(events))
		for _, m := range events {
			historyEvents = append(historyEvents, m.HistoryEvent)
		}

		if err := insertPendingEvents(ctx, tx, &targetInstance, historyEvents); err != nil {
			return fmt.Errorf("inserting messages: %w", err)
		}
	}
	return nil
}
