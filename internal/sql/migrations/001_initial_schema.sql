-- +goose Up
CREATE TABLE IF NOT EXISTS pogomq (
  auto_complete BOOLEAN NOT NULL DEFAULT FALSE,
  body JSONB NOT NULL,
  completed TIMESTAMPTZ NULL,
  delivery_count INT NOT NULL DEFAULT 0,
  id TEXT PRIMARY KEY,
  max_delivery_count INT NOT NULL DEFAULT 1,
  scheduled TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'UTC'),
  topic TEXT NOT NULL DEFAULT 'default',
  ttl_seconds BIGINT NULL,
  ttl TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_pogomq_completed ON pogomq (completed);

CREATE INDEX IF NOT EXISTS idx_pogomq_delivery_count ON pogomq (delivery_count);

CREATE INDEX IF NOT EXISTS idx_pogomq_scheduled ON pogomq (scheduled);

CREATE INDEX IF NOT EXISTS idx_pogomq_topic ON pogomq (topic);

CREATE INDEX IF NOT EXISTS idx_pogomq_ttl ON pogomq (ttl);

-- Create notification function
CREATE OR REPLACE FUNCTION notify_pogomq_changes() RETURNS TRIGGER AS $$ BEGIN IF (TG_OP = 'INSERT') OR (TG_OP = 'UPDATE' AND OLD.scheduled IS DISTINCT FROM NEW.scheduled) THEN PERFORM pg_notify('pogomq_polling_' || NEW.topic, to_char(NEW.scheduled, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')); END IF; RETURN NEW; END; $$ LANGUAGE plpgsql;

-- Create triggers for INSERT and UPDATE operations
CREATE TRIGGER notify_pogomq_changes
BEFORE INSERT OR UPDATE ON pogomq
FOR EACH ROW
EXECUTE FUNCTION notify_pogomq_changes();

-- +goose Down
DROP INDEX IF EXISTS idx_pogomq_completed;

DROP INDEX IF EXISTS idx_pogomq_delivery_count;

DROP INDEX IF EXISTS idx_pogomq_scheduled;

DROP INDEX IF EXISTS idx_pogomq_topic;

DROP INDEX IF EXISTS idx_pogomq_ttl;

DROP TABLE IF EXISTS pogomq;

DROP FUNCTION IF EXISTS notify_pogomq_changes();

DROP TRIGGER IF EXISTS notify_pogomq_changes ON pogomq;
