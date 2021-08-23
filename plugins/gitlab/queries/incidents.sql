# Incidents Table
SELECT
source,
incident_id,
MIN(IF(root.time_created < issue.time_created, root.time_created, issue.time_created)) as time_created,
MAX(time_resolved) as time_resolved,
ARRAY_AGG(root_cause IGNORE NULLS) changes,
FROM
(
SELECT 
source,
CASE WHEN event_type = "note" THEN JSON_EXTRACT_SCALAR(metadata, '$.object_attributes.noteable_id')
     WHEN event_type = "issue" THEN JSON_EXTRACT_SCALAR(metadata, '$.object_attributes.id') end as incident_id,
TIMESTAMP(JSON_EXTRACT_SCALAR(metadata, '$.object_attributes.created_at')) as time_created,
TIMESTAMP(JSON_EXTRACT_SCALAR(metadata, '$.object_attributes.closed_at')) as time_resolved,
REGEXP_EXTRACT(metadata, r"root cause: ([[:alnum:]]*)") as root_cause,
REGEXP_CONTAINS(JSON_EXTRACT(metadata, '$.object_attributes.labels'), '"title":"Incident"') as bug,
FROM four_keys.events_raw 
WHERE WHEN source LIKE "gitlab%" AND (event_type LIKE "issue%" OR (event_type = "note" and JSON_EXTRACT_SCALAR(metadata, '$.object_attributes.noteable_type') = 'Issue'))
) issue
LEFT JOIN (SELECT time_created, changes FROM four_keys.deployments d, d.changes) root on root.changes = root_cause
GROUP BY 1,2
HAVING max(bug) is True
;
