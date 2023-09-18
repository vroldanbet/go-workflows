package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/test"
	"github.com/cschleiden/go-workflows/internal/history"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func Test_PostgresBackend(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	var dbName string
	var dsn string
	test.BackendTest(t, func(options ...backend.BackendOption) test.TestBackend {
		ctx := context.Background()
		// FIXME start postgres with dockertest and change DB parameters
		dsnTemplate := "postgres://postgres:secret@localhost:5432/%s?sslmode=disable"
		dsn = fmt.Sprintf(dsnTemplate, "spicedb")
		db, err := pgx.Connect(ctx, dsn)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, db.Close(ctx))
		}()

		dbName = "test_" + strings.Replace(uuid.NewString(), "-", "", -1)
		_, err = db.Exec(ctx, "CREATE DATABASE "+dbName)
		require.NoError(t, err)

		err = db.Close(ctx)
		require.NoError(t, err)

		options = append(options, backend.WithStickyTimeout(0))

		psqlBackend, err := NewPostgresBackend(ctx, fmt.Sprintf(dsnTemplate, dbName), options...)
		require.NoError(t, err)
		return psqlBackend
	}, func(b test.TestBackend) {
		pBackend := b.(*postgresBackend)
		pBackend.pool.Close() // to prevent "another user is using database" error

		ctx := context.Background()
		db, err := pgx.Connect(ctx, dsn)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, db.Close(ctx))
		}()

		_, err = db.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName)
		require.NoError(t, err)
	})
}

func (pb *postgresBackend) GetFutureEvents(_ context.Context) ([]*history.Event, error) {
	return nil, fmt.Errorf("unimplemented")
}
