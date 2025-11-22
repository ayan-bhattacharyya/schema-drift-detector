/*
  Re-run-safe seed script implementing simplified schema-drift graph model:
   - IntegrationCatalog
   - HealingPolicy
   - NotificationPolicy
   - HealingStrategy
   - Snapshot + SnapshotField

  NOTE: This script wipes the DB at the top. Remove the wipe section if you intend to run incrementally.
*/

///// 0) Optional: full wipe (DANGEROUS) /////
MATCH (n) DETACH DELETE n;

///// 1) Create constraints (idempotent) /////
CREATE CONSTRAINT integration_catalog_pipeline_unique IF NOT EXISTS
  FOR (c:IntegrationCatalog) REQUIRE c.pipeline IS UNIQUE;

CREATE CONSTRAINT snapshot_id_unique IF NOT EXISTS
  FOR (s:Snapshot) REQUIRE s.id IS UNIQUE;

CREATE CONSTRAINT snapshotfield_id_unique IF NOT EXISTS
  FOR (sf:SnapshotField) REQUIRE sf.id IS UNIQUE;

CREATE CONSTRAINT healingstrategy_name_unique IF NOT EXISTS
  FOR (hs:HealingStrategy) REQUIRE hs.name IS UNIQUE;

CREATE CONSTRAINT notificationpolicy_id_unique IF NOT EXISTS
  FOR (np:NotificationPolicy) REQUIRE np.policy_id IS UNIQUE;

CREATE CONSTRAINT healingpolicy_id_unique IF NOT EXISTS
  FOR (hp:HealingPolicy) REQUIRE hp.policy_id IS UNIQUE;


///// 2) Insert sample HealingStrategy catalogue /////
MERGE (hs_high:HealingStrategy {name:'high'})
  ON CREATE SET hs_high.description = 'Apply strongest automatic fixes; human in loop for review', hs_high.priority = 3
  ON MATCH SET hs_high.last_seen = datetime();

MERGE (hs_medium:HealingStrategy {name:'medium'})
  ON CREATE SET hs_medium.description = 'Automated patching and notifications; may require operator approval', hs_medium.priority = 2
  ON MATCH SET hs_medium.last_seen = datetime();

MERGE (hs_low:HealingStrategy {name:'low'})
  ON CREATE SET hs_low.description = 'Informational / manual remediation suggested', hs_low.priority = 1
  ON MATCH SET hs_low.last_seen = datetime();


///// 3) Create IntegrationCatalog node for example pipeline /////
MERGE (ic:IntegrationCatalog {pipeline: 'CRM-To-Finance-PeopleData'})
  ON CREATE SET
    ic.source_system = 'CRM',
    ic.target_system = 'Finance',
    ic.integration_type = 'Batch',
    ic.source_type = 'CSV',
    ic.target_type = 'CSV',
    ic.source_component = '/Users/ayanbhattacharyya/Documents/ai-workspace/schema_drift_detector/schema_drift_detector/examples/people-info.csv',
    ic.target_component = '/Users/ayanbhattacharyya/Documents/ai-workspace/schema_drift_detector/schema_drift_detector/examples/finance-people.csv',
    ic.created = datetime()
  ON MATCH SET ic.last_seen = datetime();


///// 4) Create and link a HealingPolicy for the pipeline ///// 
// re-match ic explicitly (new statement) and create the healing policy
MATCH (ic:IntegrationCatalog {pipeline: 'CRM-To-Finance-PeopleData'})
MERGE (hp:HealingPolicy {policy_id: ic.pipeline + '|healing'})
  ON CREATE SET
     hp.pipeline = ic.pipeline,
     hp.auto_heal = true,
     hp.created = datetime()
  ON MATCH SET hp.last_seen = datetime()
MERGE (ic)-[:HAS_HEALING_POLICY]->(hp);

// attach a HealingStrategy to the HealingPolicy (use 'medium' strategy)
MATCH (hp:HealingPolicy {policy_id: 'CRM-To-Finance-PeopleData|healing'})
MERGE (hs_medium:HealingStrategy {name:'medium'})
  ON CREATE SET hs_medium.description = 'Automated patching and notifications; may require operator approval', hs_medium.priority = 2
  ON MATCH SET hs_medium.last_seen = datetime()
MERGE (hp)-[:USES_HEALING_STRATEGY]->(hs_medium);


///// 5) Create and link a NotificationPolicy for the pipeline ///// 
// re-match ic and create notification policy
MATCH (ic:IntegrationCatalog {pipeline: 'CRM-To-Finance-PeopleData'})
MERGE (np:NotificationPolicy {policy_id: ic.pipeline + '|notify'})
  ON CREATE SET
    np.pipeline = ic.pipeline,
    np.enabled = true,
    np.preferred_channel = ['email', 'teams'],
    np.email = 'finance-ops@company.com',
    np.mobile = NULL,
    np.teams_channel = 'finance-ops',
    np.created = datetime()
  ON MATCH SET np.last_seen = datetime()
MERGE (ic)-[:HAS_NOTIFICATION_POLICY]->(np);


///// 6) Create Snapshot (immutable metadata copy) and attach fields ///// 
// keep snap1 and UNWIND in the same statement so 'snap1' is in scope for the UNWIND
MATCH (ic:IntegrationCatalog {pipeline: 'CRM-To-Finance-PeopleData'})
MERGE (snap1:Snapshot {id:'snapshot_peopleinfo_v1'})
  ON CREATE SET snap1.component = 'people-info.csv',
                snap1.source_path = '/Users/ayanbhattacharyya/Documents/ai-workspace/schema_drift_detector/schema_drift_detector/examples/people-info.csv',
                snap1.timestamp = datetime(),
                snap1.created_by = 'seed-script'
  ON MATCH SET snap1.last_seen = datetime()
MERGE (ic)-[:COVERS_COMPONENT]->(snap1)
WITH snap1
UNWIND [
  {name:'name', data_type:'string', nullable:false, ordinal:0},
  {name:'date_of_birth', data_type:'date', nullable:false, ordinal:1},
  {name:'gender', data_type:'string', nullable:true, ordinal:2},
  {name:'company', data_type:'string', nullable:true, ordinal:3},
  {name:'designation', data_type:'string', nullable:true, ordinal:4}
] AS f
MERGE (sff:SnapshotField {id: snap1.id + '|' + f.name + '|' + toString(f.ordinal)})
  ON CREATE SET sff.name = f.name, sff.data_type = f.data_type, sff.nullable = f.nullable, sff.ordinal = f.ordinal
  ON MATCH SET sff.ordinal = f.ordinal
MERGE (snap1)-[:HAS_FIELD]->(sff);


///// 7) Return counts for verification /////
CALL { MATCH (c:IntegrationCatalog) RETURN count(c) AS integration_catalog_count }
CALL { MATCH (hp:HealingPolicy) RETURN count(hp) AS healing_policy_count }
CALL { MATCH (np:NotificationPolicy) RETURN count(np) AS notification_policy_count }
CALL { MATCH (s:Snapshot) RETURN count(s) AS snapshot_count }
CALL { MATCH (sf:SnapshotField) RETURN count(sf) AS snapshot_field_count }
CALL { MATCH (hs:HealingStrategy) RETURN count(hs) AS healing_strategy_count }
RETURN integration_catalog_count, healing_policy_count, notification_policy_count, snapshot_count, snapshot_field_count, healing_strategy_count;
