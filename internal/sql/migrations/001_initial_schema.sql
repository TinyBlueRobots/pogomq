-- +goose Up
CREATE TABLE IF NOT EXISTS pogomq (
  auto_complete BOOLEAN NOT NULL DEFAULT FALSE,
  body JSONB NOT NULL,
  completed TIMESTAMPTZ NULL,
  delivery_count INT NOT NULL DEFAULT 0,
  id TEXT PRIMARY KEY,
  max_delivery_count INT NOT NULL DEFAULT 1,
  scheduled TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE 'UTC'),
  topic TEXT NOT NULL DEFAULT 'default'
);

CREATE INDEX IF NOT EXISTS idx_pogomq_topic ON pogomq (topic);

CREATE INDEX IF NOT EXISTS idx_pogomq_completed ON pogomq (completed);

CREATE INDEX IF NOT EXISTS idx_pogomq_scheduled ON pogomq (scheduled);

CREATE INDEX IF NOT EXISTS idx_pogomq_delivery_count ON pogomq (delivery_count);

-- Create notification function
CREATE OR REPLACE FUNCTION notify_pogomq_changes() RETURNS TRIGGER AS $$ BEGIN IF (TG_OP = 'INSERT') THEN IF NEW.scheduled <= (NOW() AT TIME ZONE 'UTC') THEN NEW.delivery_count := NEW.delivery_count + 1; IF NEW.auto_complete THEN NEW.completed := (NOW() AT TIME ZONE 'UTC'); END IF; PERFORM pg_notify('pogomq_message_' || NEW.topic, json_build_object('id', NEW.id, 'body', NEW.body, 'delivery_count', NEW.delivery_count, 'scheduled', NEW.scheduled)::TEXT); ELSE PERFORM pg_notify('pogomq_schedule_' || NEW.topic, json_build_object('scheduled', NEW.scheduled)::TEXT); END IF; RETURN NEW; ELSIF (TG_OP = 'UPDATE') THEN IF OLD.scheduled IS DISTINCT FROM NEW.scheduled THEN IF NEW.scheduled <= (NOW() AT TIME ZONE 'UTC') THEN NEW.delivery_count := NEW.delivery_count + 1; IF NEW.auto_complete THEN NEW.completed := (NOW() AT TIME ZONE 'UTC'); END IF; PERFORM pg_notify('pogomq_message_' || NEW.topic, json_build_object('id', NEW.id, 'body', NEW.body, 'delivery_count', NEW.delivery_count, 'scheduled', NEW.scheduled)::TEXT); ELSE PERFORM pg_notify('pogomq_schedule_' || NEW.topic, json_build_object('scheduled', NEW.scheduled)::TEXT); END IF; END IF; RETURN NEW; END IF; RETURN NULL; END; $$ LANGUAGE plpgsql;

-- Create triggers for INSERT and UPDATE operations
CREATE TRIGGER notify_pogomq_changes
BEFORE INSERT OR UPDATE ON pogomq
FOR EACH ROW
EXECUTE FUNCTION notify_pogomq_changes();

-- +goose Down
DROP INDEX IF EXISTS idx_pogomq_delivery_count;

DROP INDEX IF EXISTS idx_pogomq_scheduled;

DROP INDEX IF EXISTS idx_pogomq_completed;

DROP INDEX IF EXISTS idx_pogomq_topic;

DROP TABLE IF EXISTS pogomq;

DROP FUNCTION IF EXISTS notify_pogomq_changes();

DROP TRIGGER IF EXISTS notify_pogomq_changes ON pogomq;
