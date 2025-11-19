//Get all snapshot IDs
MATCH (s:Snapshot)-[:HAS_ENTITY]->(e:Entity)
RETURN s.id AS snapshot_id, e.name AS entity, s.timestamp AS timestamp
ORDER BY timestamp DESC;


//check signgle snapshot and its Fields
// Parameter: snapshot_id -> 'snapshot_peopleinfo_v1', 'snapshot_erp_people_v1', 'snapshot_finance_people_v1'
MATCH (s:Snapshot {id: $snapshot_id})
OPTIONAL MATCH (s)-[:HAS_FIELD_COPY]->(sf:SnapshotField)
RETURN s {.*, id: s.id} AS snapshot, collect(sf {.*}) AS fields;

//Show snapshot and its entity and previous snapshot
// Parameter: snapshot_id -> '<SNAPSHOT_ID>'
MATCH (s:Snapshot {id:$snapshot_id})
OPTIONAL MATCH (s)-[:HAS_ENTITY]->(e:Entity)
OPTIONAL MATCH (s)-[:PREVIOUS_SNAPSHOT]->(prev:Snapshot)
RETURN s {.*} AS snapshot, e {.*} AS entity, prev {.*} AS previous_snapshot;

//Get latest snaphot for an entity and list its fields
// Parameter: entity_name -> 'people-info.csv'
MATCH (e:Entity {name:$entity_name})
MATCH (s:Snapshot)-[:HAS_ENTITY]->(e)
WITH s
ORDER BY s.timestamp DESC
LIMIT 1
OPTIONAL MATCH (s)-[:HAS_FIELD_COPY]->(sf:SnapshotField)
RETURN s {.*} AS latest_snapshot, collect(sf {.*}) AS fields;

//List snapshots for an entity including counts of fields per snapshot
// Parameter: entity_name -> '<ENTITY_NAME>'
MATCH (e:Entity {name:$entity_name})
MATCH (s:Snapshot)-[:HAS_ENTITY]->(e)
OPTIONAL MATCH (s)-[:HAS_FIELD_COPY]->(sf:SnapshotField)
WITH s, count(sf) AS field_count
RETURN s.id AS snapshot_id, s.timestamp AS timestamp, field_count
ORDER BY s.timestamp DESC;

//Verify SnapshotField nodes exist and view one example
// show any SnapshotField node and its properties
MATCH (sf:SnapshotField)
RETURN sf {.*} LIMIT 20;

//Find SnapshotField nodes that are NOT attached to any Snapshot (orphans)
// If this returns > 0, earlier code may have created fields but not linked them.
MATCH (sf:SnapshotField)
WHERE NOT ( ()-[:HAS_FIELD_COPY]->(sf) )
RETURN count(sf) AS orphan_field_count, collect(sf {id:sf.id, name:sf.name})[0..30] AS sample_orphans;

//Check ETL jobs that reference the entity (impacted jobs)
// Parameter: entity_name -> '<ENTITY_NAME>'
MATCH (j:ETLJob)-[:USES_SOURCE|:PRODUCES]->(e:Entity {name:$entity_name})
RETURN DISTINCT j.job_id AS job_id, j.auto_heal_allowed AS auto_heal_allowed, j.notify_on_breaking AS notify_on_breaking;

//Inspect hints_json stored on SnapshotField and parse (if stored as JSON string)
// Parameter: snapshot_id -> '<SNAPSHOT_ID>'
MATCH (s:Snapshot {id:$snapshot_id})-[:HAS_FIELD_COPY]->(sf:SnapshotField)
RETURN sf.name AS field_name, sf.data_type AS data_type, sf.hints_json AS hints_json
LIMIT 100;

