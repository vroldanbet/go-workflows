CREATE TABLE IF NOT EXISTS instances (
  id SERIAL PRIMARY KEY,
  instance_id VARCHAR NOT NULL,
  execution_id VARCHAR NOT NULL,
  parent_instance_id VARCHAR NULL,
  parent_execution_id VARCHAR NULL,
  parent_schedule_event_id BIGINT NULL,
  metadata BYTEA NULL,
  state INT NOT NULL,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL,
  completed_at TIMESTAMP WITHOUT TIME ZONE NULL,
  locked_until TIMESTAMP WITHOUT TIME ZONE NULL,
  sticky_until TIMESTAMP WITHOUT TIME ZONE NULL,
  worker VARCHAR(64) NULL,

  CONSTRAINT uq_instances_instance_id_execution_id UNIQUE (instance_id, execution_id)
);

CREATE INDEX idx_instances_locked_until_completed_at ON instances (completed_at, locked_until, sticky_until, worker);
CREATE INDEX idx_instances_parent_instance_id_parent_execution_id ON instances (parent_instance_id, parent_execution_id);

CREATE TABLE IF NOT EXISTS pending_events (
  id SERIAL PRIMARY KEY,
  event_id VARCHAR NOT NULL,
  sequence_id BIGINT NOT NULL, -- Not used, but keep for now for query compat
  instance_id VARCHAR NOT NULL,
  execution_id VARCHAR NOT NULL,
  event_type INT NOT NULL,
  timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  schedule_event_id BIGINT NOT NULL,
  attributes BYTEA NOT NULL,
  visible_at TIMESTAMP WITHOUT TIME ZONE NULL
);

CREATE INDEX idx_pending_events_inid_exid ON pending_events (instance_id, execution_id);
CREATE INDEX idx_pending_events_inid_exid_visible_at_schedule_event_id ON pending_events (instance_id, execution_id, visible_at, schedule_event_id);

CREATE TABLE IF NOT EXISTS history (
  id SERIAL PRIMARY KEY,
  event_id VARCHAR NOT NULL,
  sequence_id BIGINT NOT NULL,
  instance_id VARCHAR NOT NULL,
  execution_id VARCHAR NOT NULL,
  event_type INT NOT NULL,
  timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  schedule_event_id BIGINT NOT NULL,
  attributes BYTEA NOT NULL,
  visible_at TIMESTAMP WITHOUT TIME ZONE NULL -- Is this required?
);

CREATE INDEX idx_history_instance_id_execution_id ON history (instance_id, execution_id);
CREATE INDEX idx_history_instance_id_execution_id_sequence_id ON history (instance_id, execution_id, sequence_id);

CREATE TABLE IF NOT EXISTS activities (
  id SERIAL PRIMARY KEY,
  activity_id VARCHAR NOT NULL,
  instance_id VARCHAR NOT NULL,
  execution_id VARCHAR NOT NULL,
  event_type INT NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  schedule_event_id BIGINT NOT NULL,
  attributes BYTEA NOT NULL,
  visible_at TIMESTAMP NULL,
  locked_until TIMESTAMP NULL,
  worker VARCHAR NULL,

  CONSTRAINT idx_activities_instance_id_execution_id_activity_id_worker UNIQUE (instance_id, execution_id, activity_id, worker)
);

CREATE INDEX idx_activities_locked_until ON activities (locked_until);
