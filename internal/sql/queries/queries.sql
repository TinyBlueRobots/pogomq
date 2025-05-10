-- name: EnqueueMessages :copyfrom
INSERT INTO pogomq (
  auto_complete, 
  body, 
  id,
  max_delivery_count,
  scheduled, 
  topic
) VALUES (
  $1, $2, $3, $4, $5, $6
);

-- name: CompleteMessage :exec
UPDATE
  pogomq
SET
  completed = (NOW() AT TIME ZONE 'UTC')
WHERE
  id = @id
  AND completed IS NULL;

-- name: DeleteMessage :exec
DELETE FROM
  pogomq
WHERE
  id = @id;

-- name: FailMessage :exec
UPDATE
  pogomq
SET
  completed = NULL,
  scheduled = @scheduled
WHERE
  id = @id
  AND delivery_count < max_delivery_count;

-- name: RescheduleMessages :exec
UPDATE
  pogomq
SET
  scheduled = (NOW() AT TIME ZONE 'UTC') 
WHERE
  topic = @topic
  AND completed IS NULL
  AND delivery_count < max_delivery_count
  AND scheduled < (NOW() AT TIME ZONE 'UTC');

-- name: ResetFailedMessages :exec
UPDATE
  pogomq
SET
  completed = NULL,
  scheduled = (NOW() AT TIME ZONE 'UTC'),
  delivery_count = 0
WHERE
  topic = @topic
  AND completed IS NOT NULL;

-- name: PurgeMessages :exec
DELETE FROM
  pogomq
WHERE
  completed < @before
  AND topic = @topic;

-- name: MessageCounts :one
WITH message_counts AS (
  SELECT
    CASE
      WHEN completed IS NULL AND delivery_count < max_delivery_count AND scheduled <= (NOW() AT TIME ZONE 'UTC') THEN 'active'
      WHEN completed IS NULL AND delivery_count >= max_delivery_count THEN 'failed'
      WHEN completed IS NULL AND scheduled > (NOW() AT TIME ZONE 'UTC') THEN 'scheduled'
      WHEN completed IS NOT NULL THEN 'completed'
      ELSE 'unknown'
    END AS message_state
  FROM
    pogomq
  WHERE
    topic = @topic
)
SELECT
  COALESCE(SUM(CASE WHEN message_state = 'active' THEN 1 ELSE 0 END), 0) :: INT AS active_count,
  COALESCE(SUM(CASE WHEN message_state = 'failed' THEN 1 ELSE 0 END), 0) :: INT AS failed_count,
  COALESCE(SUM(CASE WHEN message_state = 'scheduled' THEN 1 ELSE 0 END), 0) :: INT AS scheduled_count,
  COALESCE(SUM(CASE WHEN message_state = 'completed' THEN 1 ELSE 0 END), 0) :: INT AS completed_count
FROM
  message_counts;