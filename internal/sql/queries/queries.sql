-- name: EnqueueMessages :copyfrom
INSERT INTO pogomq (
  auto_complete, 
  body, 
  id,
  max_delivery_count,
  scheduled, 
  topic,
  ttl,
  ttl_seconds
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8
);

-- name: CompleteMessage :exec
UPDATE
  pogomq
SET
  completed = (NOW() AT TIME ZONE 'UTC'),
  ttl = CASE WHEN ttl_seconds IS NOT NULL THEN (NOW() AT TIME ZONE 'UTC') + (ttl_seconds * INTERVAL '1 second') ELSE NULL END
WHERE
  id = @id
  AND completed IS NULL;

-- name: DeleteMessage :exec
DELETE FROM
  pogomq
WHERE
  id = @id;

-- name: ReadMessages :many
WITH next_scheduled AS (
  SELECT MIN(scheduled) as next_time
  FROM pogomq
  WHERE 
    pogomq.topic = @topic
    AND completed IS NULL
    AND scheduled > (NOW() AT TIME ZONE 'UTC')
    AND delivery_count < max_delivery_count
),
updates AS (
  UPDATE pogomq
  SET 
    delivery_count = delivery_count + 1,
    completed = CASE WHEN auto_complete THEN (NOW() AT TIME ZONE 'UTC') ELSE NULL END,
    ttl = CASE WHEN ttl_seconds IS NOT NULL THEN (NOW() AT TIME ZONE 'UTC') + (ttl_seconds * INTERVAL '1 second') ELSE NULL END
  WHERE
    ctid IN (
      SELECT ctid
      FROM pogomq
      WHERE 
        pogomq.topic = @topic
        AND completed IS NULL
        AND scheduled <= (NOW() AT TIME ZONE 'UTC')
        AND delivery_count < max_delivery_count
      ORDER BY
        scheduled ASC
      FOR UPDATE SKIP LOCKED
      LIMIT $1
    )
  RETURNING 
    id, 
    body, 
    delivery_count,
    scheduled
)
SELECT 
  u.id, 
  u.body, 
  u.delivery_count,
  u.scheduled,
  (SELECT next_time FROM next_scheduled) :: TIMESTAMPTZ as next_time
FROM updates u
UNION ALL
SELECT 
  '' as id,
  NULL as body,
  0 as delivery_count,
  NULL as scheduled,
  (SELECT next_time FROM next_scheduled) :: TIMESTAMPTZ as next_time
WHERE NOT EXISTS (SELECT 1 FROM updates)
LIMIT $1;

-- name: ReadFailedMessages :many
SELECT
  id,
  body,
  delivery_count,
  scheduled
FROM
  pogomq
WHERE
  topic = @topic
  AND completed IS NULL
  AND delivery_count >= max_delivery_count
ORDER BY
  scheduled ASC
LIMIT $1;

-- name: FailMessage :exec
UPDATE
  pogomq
SET
  completed = NULL,
  scheduled = @scheduled,
  ttl = CASE WHEN ttl_seconds IS NOT NULL THEN (NOW() AT TIME ZONE 'UTC') + (ttl_seconds * INTERVAL '1 second') ELSE NULL END
WHERE
  id = @id
  AND delivery_count < max_delivery_count;

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

-- name: ResetFailedMessage :exec
UPDATE
  pogomq
SET
  completed = NULL,
  scheduled = (NOW() AT TIME ZONE 'UTC'),
  delivery_count = 0
WHERE
  id = @id
  AND topic = @topic
  AND completed IS NOT NULL;

-- name: PurgeTTLMessages :exec
DELETE FROM
  pogomq
WHERE
  ttl IS NOT NULL
  AND ttl < (NOW() AT TIME ZONE 'UTC')
  AND topic = @topic;

-- name: PurgeCompletedMessages :exec
DELETE FROM
  pogomq
WHERE
  completed IS NOT NULL
  AND topic = @topic;

-- name: PurgeFailedMessages :exec
DELETE FROM
  pogomq
WHERE
  completed IS NULL
  AND delivery_count >= max_delivery_count
  AND topic = @topic;

-- name: PurgeAllMessages :exec
DELETE FROM
  pogomq
WHERE
  topic = @topic;

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