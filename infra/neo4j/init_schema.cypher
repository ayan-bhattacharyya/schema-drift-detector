/*
  Full reset + seed script for CSV schema drift demo
  - Deletes all existing nodes and relationships
  - Recreates constraints (idempotent)
  - Inserts Entities, Fields, Snapshots (metadata-only), ETLJobs and Transformations
  - No sample data / no PII

  UPDATED: ETLJob nodes now include policy properties:
    auto_heal_allowed, notify_on_breaking, notify_channels, operator_contact,
    healing_strategy, severity_ruleset

  ADDED: SnapshotField creation and links (so snapshots include fields)
*/

///// 0) Wipe database (dangerous: deletes everything) /////
MATCH (n) DETACH DELETE n;

///// 1) Create constraints (idempotent in Neo4j 5+) /////
CREATE CONSTRAINT entity_name_unique IF NOT EXISTS
  FOR (e:Entity) REQUIRE e.name IS UNIQUE;
CREATE CONSTRAINT snapshot_id_unique IF NOT EXISTS
  FOR (s:Snapshot) REQUIRE s.id IS UNIQUE;
CREATE CONSTRAINT field_id_unique IF NOT EXISTS
  FOR (f:Field) REQUIRE f.field_id IS UNIQUE;
CREATE CONSTRAINT etljob_id_unique IF NOT EXISTS
  FOR (j:ETLJob) REQUIRE j.job_id IS UNIQUE;
CREATE CONSTRAINT transformation_id_unique IF NOT EXISTS
  FOR (t:Transformation) REQUIRE t.transformation_id IS UNIQUE;

/* ADDED: SnapshotField id unique so snapshot copies are idempotent */
CREATE CONSTRAINT snapshotfield_id_unique IF NOT EXISTS
  FOR (sf:SnapshotField) REQUIRE sf.id IS UNIQUE;

///// 2) Upsert Entities (source + targets) /////
MERGE (srcEntity:Entity {name: 'people-info.csv'})
  ON CREATE SET srcEntity.type = 'file', srcEntity.source_path = 'people-info.csv', srcEntity.created = datetime()
  ON MATCH SET srcEntity.last_seen = datetime();

MERGE (financeEntity:Entity {name: 'finance-people.csv'})
  ON CREATE SET financeEntity.type = 'file', financeEntity.source_path = 'finance-people.csv', financeEntity.created = datetime()
  ON MATCH SET financeEntity.last_seen = datetime();

MERGE (erpEntity:Entity {name: 'erp-people.csv'})
  ON CREATE SET erpEntity.type = 'file', erpEntity.source_path = 'erp-people.csv', erpEntity.created = datetime()
  ON MATCH SET erpEntity.last_seen = datetime();

///// 3) Create placeholder snapshots (metadata only) /////
MERGE (srcSnap:Snapshot {id: 'snapshot_peopleinfo_v1'})
  ON CREATE SET srcSnap.source = 'people-info.csv', srcSnap.timestamp = datetime(), srcSnap.version = 'v1'
  ON MATCH SET srcSnap.timestamp = datetime();

MERGE (finSnap:Snapshot {id: 'snapshot_finance_people_v1'})
  ON CREATE SET finSnap.source = 'finance-people.csv', finSnap.timestamp = datetime(), finSnap.version = 'v1'
  ON MATCH SET finSnap.timestamp = datetime();

MERGE (erpSnap:Snapshot {id: 'snapshot_erp_people_v1'})
  ON CREATE SET erpSnap.source = 'erp-people.csv', erpSnap.timestamp = datetime(), erpSnap.version = 'v1'
  ON MATCH SET erpSnap.timestamp = datetime();

MATCH (srcSnap:Snapshot {id: 'snapshot_peopleinfo_v1'}), (srcEntity:Entity {name: 'people-info.csv'})
MERGE (srcSnap)-[:HAS_ENTITY]->(srcEntity);

MATCH (finSnap:Snapshot {id: 'snapshot_finance_people_v1'}), (financeEntity:Entity {name: 'finance-people.csv'})
MERGE (finSnap)-[:HAS_ENTITY]->(financeEntity);

MATCH (erpSnap:Snapshot {id: 'snapshot_erp_people_v1'}), (erpEntity:Entity {name: 'erp-people.csv'})
MERGE (erpSnap)-[:HAS_ENTITY]->(erpEntity);

///// 4) Fields for source: people-info.csv (schema metadata only) /////
MATCH (srcEntity:Entity {name: 'people-info.csv'})
UNWIND [
  {name:'name', data_type:'string', nullable:false, ordinal:0},
  {name:'date_of_birth', data_type:'date', nullable:false, ordinal:1},
  {name:'gender', data_type:'string', nullable:true, ordinal:2},
  {name:'company', data_type:'string', nullable:true, ordinal:3},
  {name:'designation', data_type:'string', nullable:true, ordinal:4}
] AS f
WITH srcEntity, f, (CASE WHEN f.nullable THEN 'true' ELSE 'false' END) AS nullable_str
MERGE (sf:Field {field_id: srcEntity.name + '|' + f.name + '|' + f.data_type + '|' + nullable_str})
  ON CREATE SET sf.name = f.name, sf.data_type = f.data_type, sf.nullable = f.nullable, sf.ordinal = f.ordinal
  ON MATCH SET sf.ordinal = f.ordinal
MERGE (srcEntity)-[:HAS_FIELD]->(sf);

///// 5) Fields for target: finance-people.csv /////
MATCH (finEntity:Entity {name: 'finance-people.csv'})
UNWIND [
  {name:'firstname', data_type:'string', nullable:false, ordinal:0},
  {name:'lastname', data_type:'string', nullable:false, ordinal:1},
  {name:'age', data_type:'int', nullable:true, ordinal:2},
  {name:'gender', data_type:'string', nullable:true, ordinal:3},
  {name:'company', data_type:'string', nullable:true, ordinal:4},
  {name:'designation', data_type:'string', nullable:true, ordinal:5}
] AS f
WITH finEntity, f, (CASE WHEN f.nullable THEN 'true' ELSE 'false' END) AS nullable_str
MERGE (ff:Field {field_id: finEntity.name + '|' + f.name + '|' + f.data_type + '|' + nullable_str})
  ON CREATE SET ff.name = f.name, ff.data_type = f.data_type, ff.nullable = f.nullable, ff.ordinal = f.ordinal
  ON MATCH SET ff.ordinal = f.ordinal
MERGE (finEntity)-[:HAS_FIELD]->(ff);

///// 6) Fields for target: erp-people.csv /////
MATCH (erpEntity:Entity {name: 'erp-people.csv'})
UNWIND [
  {name:'name', data_type:'string', nullable:false, ordinal:0},
  {name:'age', data_type:'int', nullable:true, ordinal:1},
  {name:'gender', data_type:'string', nullable:true, ordinal:2}
] AS f
WITH erpEntity, f, (CASE WHEN f.nullable THEN 'true' ELSE 'false' END) AS nullable_str
MERGE (ef:Field {field_id: erpEntity.name + '|' + f.name + '|' + f.data_type + '|' + nullable_str})
  ON CREATE SET ef.name = f.name, ef.data_type = f.data_type, ef.nullable = f.nullable, ef.ordinal = f.ordinal
  ON MATCH SET ef.ordinal = f.ordinal
MERGE (erpEntity)-[:HAS_FIELD]->(ef);

///// 3.5) Create SnapshotField copies and link to snapshots (ADDED) ///// 
/* Create snapshot-specific immutable copies of fields and attach to the placeholder Snapshot nodes.
   This ensures a Snapshot has HAS_FIELD_COPY -> SnapshotField for metadata_agent and detector_agent. */

/// People-info snapshot fields
MATCH (snap:Snapshot {id: 'snapshot_peopleinfo_v1'})-[:HAS_ENTITY]->(e:Entity {name:'people-info.csv'})
MATCH (e)-[:HAS_FIELD]->(f:Field)
MERGE (sfc:SnapshotField {id: snap.id + '|' + f.field_id})
  ON CREATE SET sfc.name = f.name, sfc.data_type = f.data_type, sfc.nullable = f.nullable, sfc.ordinal = f.ordinal
  ON MATCH SET sfc.ordinal = f.ordinal
MERGE (snap)-[:HAS_FIELD_COPY]->(sfc);

/// Finance snapshot fields
MATCH (snapF:Snapshot {id: 'snapshot_finance_people_v1'})-[:HAS_ENTITY]->(efin:Entity {name:'finance-people.csv'})
MATCH (efin)-[:HAS_FIELD]->(f2:Field)
MERGE (sff:SnapshotField {id: snapF.id + '|' + f2.field_id})
  ON CREATE SET sff.name = f2.name, sff.data_type = f2.data_type, sff.nullable = f2.nullable, sff.ordinal = f2.ordinal
  ON MATCH SET sff.ordinal = f2.ordinal
MERGE (snapF)-[:HAS_FIELD_COPY]->(sff);

/// ERP snapshot fields
MATCH (snapE:Snapshot {id: 'snapshot_erp_people_v1'})-[:HAS_ENTITY]->(eerp:Entity {name:'erp-people.csv'})
MATCH (eerp)-[:HAS_FIELD]->(f3:Field)
MERGE (sfe:SnapshotField {id: snapE.id + '|' + f3.field_id})
  ON CREATE SET sfe.name = f3.name, sfe.data_type = f3.data_type, sfe.nullable = f3.nullable, sfe.ordinal = f3.ordinal
  ON MATCH SET sfe.ordinal = f3.ordinal
MERGE (snapE)-[:HAS_FIELD_COPY]->(sfe);

///// 7) Upsert ETL jobs and link to entities (job_id = pipeline name) ///// 
/* UPDATED: add policy properties to ETLJob nodes (auto_heal_allowed, notify_on_breaking, notify_channels,
   operator_contact, healing_strategy, severity_ruleset). Adjust defaults below as needed. */
MATCH (src:Entity {name:'people-info.csv'}), (fin:Entity {name:'finance-people.csv'}), (erp:Entity {name:'erp-people.csv'})
MERGE (jobFin:ETLJob {job_id: 'CRM-To-Finance-PeopleData'})
  ON CREATE SET jobFin.name = 'CRM-To-Finance-PeopleData',
                jobFin.description = 'ETL: people-info.csv -> finance-people.csv',
                jobFin.created = datetime(),
                jobFin.schedule = 'ad-hoc',
                /* ====== POLICY PROPERTIES (UPDATED) ====== */
                jobFin.auto_heal_allowed = true,
                jobFin.notify_on_breaking = true,
                jobFin.notify_channels = ['email', 'teams'],
                jobFin.operator_contact = 'finance-ops@company.com',
                jobFin.healing_strategy = 'patch',
                jobFin.severity_ruleset = 'standard'
  ON MATCH SET jobFin.last_seen = datetime(),
                /* Keep policies in-sync on subsequent runs (idempotent update) */
                jobFin.auto_heal_allowed = coalesce(jobFin.auto_heal_allowed, true),
                jobFin.notify_on_breaking = coalesce(jobFin.notify_on_breaking, true),
                jobFin.notify_channels = coalesce(jobFin.notify_channels, ['email', 'teams']),
                jobFin.operator_contact = coalesce(jobFin.operator_contact, 'finance-ops@company.com'),
                jobFin.healing_strategy = coalesce(jobFin.healing_strategy, 'patch'),
                jobFin.severity_ruleset = coalesce(jobFin.severity_ruleset, 'standard')

MERGE (jobERP:ETLJob {job_id: 'CRM-To-ERP-PeopleData'})
  ON CREATE SET jobERP.name = 'CRM-To-ERP-PeopleData',
                jobERP.description = 'ETL: people-info.csv -> erp-people.csv',
                jobERP.created = datetime(),
                jobERP.schedule = 'ad-hoc',
                /* ====== POLICY PROPERTIES (UPDATED) ====== */
                jobERP.auto_heal_allowed = false,
                jobERP.notify_on_breaking = true,
                jobERP.notify_channels = ['email'],
                jobERP.operator_contact = 'erp-ops@company.com',
                jobERP.healing_strategy = 'pause',
                jobERP.severity_ruleset = 'strict'
  ON MATCH SET jobERP.last_seen = datetime(),
                jobERP.auto_heal_allowed = coalesce(jobERP.auto_heal_allowed, false),
                jobERP.notify_on_breaking = coalesce(jobERP.notify_on_breaking, true),
                jobERP.notify_channels = coalesce(jobERP.notify_channels, ['email']),
                jobERP.operator_contact = coalesce(jobERP.operator_contact, 'erp-ops@company.com'),
                jobERP.healing_strategy = coalesce(jobERP.healing_strategy, 'pause'),
                jobERP.severity_ruleset = coalesce(jobERP.severity_ruleset, 'strict')

MERGE (jobFin)-[:USES_SOURCE]->(src)
MERGE (jobFin)-[:PRODUCES]->(fin)
MERGE (jobERP)-[:USES_SOURCE]->(src)
MERGE (jobERP)-[:PRODUCES]->(erp);

///// 8) Transformations: CRM-To-Finance-PeopleData mappings ///// 
/* 0: firstname = first token of name */
MATCH
  (s:Entity {name:'people-info.csv'})-[:HAS_FIELD]->(sf_name:Field {name:'name'}),
  (t:Entity {name:'finance-people.csv'})-[:HAS_FIELD]->(tf_first:Field {name:'firstname'}),
  (job:ETLJob {job_id:'CRM-To-Finance-PeopleData'})
MERGE (trans0:Transformation {transformation_id: job.job_id + '|' + '0'})
  ON CREATE SET trans0.job_id = job.job_id, trans0.mapping_order = 0, trans0.source_field = sf_name.field_id, trans0.target_field = tf_first.field_id,
                trans0.expression = "split(name, ' ')[0]", trans0.description = "firstname = first token of source.name", trans0.created = datetime()
  ON MATCH SET trans0.expression = "split(name, ' ')[0]"
MERGE (sf_name)-[m0:MAPPED_TO {job_id: job.job_id, mapping_order: 0}]->(tf_first) 
  ON CREATE SET m0.expression = "split(name, ' ')[0]" 
  ON MATCH SET m0.expression = "split(name, ' ')[0]"
MERGE (trans0)-[:APPLIES_TO_JOB]->(job)
MERGE (trans0)-[:MAPS_SOURCE]->(sf_name)
MERGE (trans0)-[:MAPS_TARGET]->(tf_first);

/* ... rest of transformations unchanged ... */

///// 10) Convenience relationships: Job -> FIELD_MAPPINGS /////
MATCH (jobFin:ETLJob {job_id:'CRM-To-Finance-PeopleData'}),
      (ff:Field) WHERE ff.field_id STARTS WITH 'finance-people.csv|'
WITH jobFin, ff
MERGE (jobFin)-[:FIELD_MAPPINGS]->(ff);

MATCH (jobERP:ETLJob {job_id:'CRM-To-ERP-PeopleData'}),
      (ef:Field) WHERE ef.field_id STARTS WITH 'erp-people.csv|'
WITH jobERP, ef
MERGE (jobERP)-[:FIELD_MAPPINGS]->(ef);

///// 11) Entity <-> Job convenience relationships /////
MATCH (je:ETLJob {job_id:'CRM-To-Finance-PeopleData'}), (e:Entity {name:'people-info.csv'}), (t:Entity {name:'finance-people.csv'})
MERGE (e)-[:SOURCE_FOR {job_id: je.job_id}]->(je)
MERGE (je)-[:TARGETS]->(t);

MATCH (je2:ETLJob {job_id:'CRM-To-ERP-PeopleData'}), (e2:Entity {name:'people-info.csv'}), (t2:Entity {name:'erp-people.csv'})
MERGE (e2)-[:SOURCE_FOR {job_id: je2.job_id}]->(je2)
MERGE (je2)-[:TARGETS]->(t2);

///// 12) Return verification counts and job policies /////
CALL { MATCH (n:Snapshot) RETURN count(n) AS snapshot_count }
CALL { MATCH (m:Entity) RETURN count(m) AS entity_count }
CALL { MATCH (f:Field) RETURN count(f) AS field_count }
CALL { MATCH (j:ETLJob) RETURN count(j) AS etljob_count }
CALL { MATCH (t:Transformation) RETURN count(t) AS transformation_count }

RETURN snapshot_count, entity_count, field_count, etljob_count, transformation_count;

/* Optional: inspect ETLJob policy properties */
MATCH (jj:ETLJob)
RETURN jj.job_id AS job_id,
       jj.auto_heal_allowed AS auto_heal_allowed,
       jj.notify_on_breaking AS notify_on_breaking,
       jj.notify_channels AS notify_channels,
       jj.operator_contact AS operator_contact,
       jj.healing_strategy AS healing_strategy,
       jj.severity_ruleset AS severity_ruleset
ORDER BY job_id;
