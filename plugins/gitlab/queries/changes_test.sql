with events_raw as (
    select *
    from unnest([
        struct
            ("A" as id, "gitlab" as source, "pull_request" as event_type, '{"commits": [{"id": "c1", "timestamp": "2020-11-23 09:15:00.042308 UTC"}]}' as metadata, timestamp '2020-11-23 09:15:00.042308 UTC' as time_created, "s1" as signature, "m1" as msg_id),
            ("B", "gitlab", "pull_request", '{"commits": [{"id": "c2", "timestamp": "2020-11-24 09:15:00.042308 UTC"}, {"id": "c3", "timestamp": "2020-11-24 09:16:00.042308 UTC"}]}', timestamp '2020-11-24 09:15:00.042308 UTC', "s2", "m2")
        ]
        )
),
derived_table as (
$changes
),
expected_table as (
    select *
    from unnest([
        struct
         ("gitlab" as source, "pull_request" as event_type, "c1" as change_id, TIMESTAMP_TRUNC(TIMESTAMP('2020-11-23 09:15:00.042308 UTC'), second) as time_created),
         ("gitlab", "pull_request", "c2", TIMESTAMP_TRUNC(TIMESTAMP('2020-11-24 09:15:00.042308 UTC'), second)),
         ("gitlab", "pull_request", "c4", TIMESTAMP_TRUNC(TIMESTAMP('2020-11-24 09:16:00.042308 UTC'), second))
        ]
        )
)

# Return distinct lines between derived and expected tables. If any lines are returned, the test fails.
(
    SELECT *
    FROM derived_changes
        EXCEPT DISTINCT
    SELECT *
    from expected_changes
)
UNION ALL
(
    SELECT *
    FROM expected_changes
        EXCEPT DISTINCT
    SELECT *
    from derived_changes
)
