# Deployments Table

WITH deploys AS (
      SELECT 
      source,
      id as deploy_id,
      time_created,
      JSON_EXTRACT_SCALAR(metadata, '$.commit.id')as main_commit,
      ARRAY<string>[] as additional_commits
      FROM four_keys.events_raw 
      WHERE source LIKE "gitlab%" and event_type = "pipeline" and JSON_EXTRACT_SCALAR(metadata, '$.object_attributes.status') = "success"
    ),
    changes_raw AS (
      SELECT
      id,
      metadata as change_metadata
      FROM four_keys.events_raw
    ),
    deployment_changes as (
      SELECT
      source,
      deploy_id,
      deploys.time_created time_created,
      change_metadata,
      four_keys.json2array(JSON_EXTRACT(change_metadata, '$.commits')) as array_commits,
      main_commit
      FROM deploys
      JOIN
        changes_raw on (
          changes_raw.id = deploys.main_commit
          or changes_raw.id in unnest(deploys.additional_commits)
        )
    )

    SELECT 
    source,
    deploy_id,
    time_created,
    main_commit,   
    ARRAY_AGG(DISTINCT JSON_EXTRACT_SCALAR(array_commits, '$.id')) changes,    
    FROM deployment_changes
    CROSS JOIN deployment_changes.array_commits
    GROUP BY 1,2,3,4;
