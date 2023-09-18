package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/cschleiden/go-workflows/diag"
	"github.com/cschleiden/go-workflows/internal/core"
	"github.com/jackc/pgx/v5"
)

func (pb *postgresBackend) GetWorkflowInstances(ctx context.Context, afterInstanceID, afterExecutionID string, count int) ([]*diag.WorkflowInstanceRef, error) {
	var instances []*diag.WorkflowInstanceRef

	err := pb.Tx(ctx, func(tx pgx.Tx) error {
		var rows pgx.Rows
		var err error
		if afterInstanceID != "" {
			rows, err = tx.Query(
				ctx,
				`SELECT i.instance_id, i.execution_id, i.created_at, i.completed_at
			FROM instances i
			INNER JOIN (SELECT instance_id, created_at FROM instances WHERE id = $1 AND execution_id = $2) ii
				ON i.created_at < ii.created_at OR (i.created_at = ii.created_at AND i.instance_id < ii.instance_id)
			ORDER BY i.created_at DESC, i.instance_id DESC
			LIMIT $3`,
				afterInstanceID,
				afterExecutionID,
				count,
			)
		} else {
			rows, err = tx.Query(
				ctx,
				`SELECT i.instance_id, i.execution_id, i.created_at, i.completed_at
			FROM instances i
			ORDER BY i.created_at DESC, i.instance_id DESC
			LIMIT $1`,
				count,
			)
		}
		if err != nil {
			return err
		}

		for rows.Next() {
			var id, executionID string
			var createdAt time.Time
			var completedAt *time.Time
			err = rows.Scan(&id, &executionID, &createdAt, &completedAt)
			if err != nil {
				return err
			}

			var state core.WorkflowInstanceState
			if completedAt != nil {
				state = core.WorkflowInstanceStateFinished
			}

			instances = append(instances, &diag.WorkflowInstanceRef{
				Instance:    core.NewWorkflowInstance(id, executionID),
				CreatedAt:   createdAt,
				CompletedAt: completedAt,
				State:       state,
			})
		}

		return nil
	})

	return instances, err
}

func (pb *postgresBackend) GetWorkflowInstance(ctx context.Context, instance *core.WorkflowInstance) (*diag.WorkflowInstanceRef, error) {
	var instanceRef *diag.WorkflowInstanceRef
	err := pb.Tx(ctx, func(tx pgx.Tx) error {
		res := tx.QueryRow(
			ctx,
			"SELECT instance_id, execution_id, created_at, completed_at FROM instances WHERE instance_id = $1 AND execution_id = $2", instance.InstanceID, instance.ExecutionID)

		var id, executionID string
		var createdAt time.Time
		var completedAt *time.Time

		err := res.Scan(&id, &executionID, &createdAt, &completedAt)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}

			return err
		}

		var state core.WorkflowInstanceState
		if completedAt != nil {
			state = core.WorkflowInstanceStateFinished
		}

		instanceRef = &diag.WorkflowInstanceRef{
			Instance:    core.NewWorkflowInstance(id, executionID),
			CreatedAt:   createdAt,
			CompletedAt: completedAt,
			State:       state,
		}

		return nil
	})

	return instanceRef, err
}

func (pb *postgresBackend) GetWorkflowTree(ctx context.Context, instance *core.WorkflowInstance) (*diag.WorkflowInstanceTree, error) {
	//TODO implement me
	panic("implement me")
}
