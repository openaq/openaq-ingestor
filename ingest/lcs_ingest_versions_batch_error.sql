UPDATE fetchlogs
SET completed_datetime = now()
, last_message = 'ERROR: batch error'
FROM ms_versions v
WHERE v.fetchlogs_id = fetchlogs.fetchlogs_id;

INSERT INTO rejects (tbl, r, fetchlogs_id)
SELECT 'versions-batch-error'
, row_to_json(v.*)
, fetchlogs_id
FROM ms_versions v;
