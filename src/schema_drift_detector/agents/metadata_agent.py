# src/schema_drift/agents/metadata_agent.py
from __future__ import annotations

import os
import uuid
import json
import logging
import datetime
from typing import Any, Dict, List, Optional

from neo4j import GraphDatabase
from dotenv import load_dotenv

try:
    from neo4j.exceptions import Neo4jError
except Exception:  # pragma: no cover - defensive fallback
    Neo4jError = Exception

logger = logging.getLogger("metadata_agent")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

load_dotenv()


def get_env(key: str, default=None, required=False):
    value = os.getenv(key, default)
    if required and (value is None or str(value).strip() == ""):
        raise EnvironmentError(f"Environment variable '{key}' is required but not set.")
    return value


NEO4J_URI = get_env("NEO4J_URI", required=True)
NEO4J_USER = get_env("NEO4J_USER", required=True)
NEO4J_PASSWORD = get_env("NEO4J_PASSWORD", required=True)
NEO4J_DB = get_env("NEO4J_DB", required=True)


def _driver():
    # If NEO4J_DB is provided the session can specify database=NEO4J_DB when opening
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# Keys that must NOT be present in snapshots (no PII / sample rows)
FORBIDDEN_SNAPSHOT_KEYS = {"sample_rows", "rows", "data", "example_values", "example", "samples"}


def iso_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _ensure_no_forbidden(o: Any) -> None:
    """
    Recursively validate that no forbidden keys are present.
    Raises ValueError if a forbidden key is found.
    """
    if isinstance(o, dict):
        for k, v in o.items():
            if k in FORBIDDEN_SNAPSHOT_KEYS:
                raise ValueError(f"Forbidden snapshot key encountered: {k}")
            _ensure_no_forbidden(v)
    elif isinstance(o, (list, tuple, set)):
        for itm in o:
            _ensure_no_forbidden(itm)


class MetadataAgent:
    """
    Metadata agent persists canonical snapshots into Neo4j and manages version links.
    """

    def __init__(self) -> None:
        logger.info("Connecting to Neo4j at %s", NEO4J_URI)
        # create driver
        self._driver = _driver()

    def close(self) -> None:
        try:
            self._driver.close()
        except Exception:
            logger.exception("Error closing Neo4j driver")

    def persist_snapshot(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Persist snapshot payload and return metadata including snapshot id and impacted jobs.

        payload must include:
        - request_id (optional)
        - snapshot: {
            source_id, entity, schema: { fields: [...], version_meta: {...} }
          }
        """
        request_id = payload.get("request_id")
        snapshot = payload.get("snapshot") or payload.get("schema") or {}
        if not snapshot:
            raise ValueError("Payload missing 'snapshot' object")

        # Safety checks
        _ensure_no_forbidden(snapshot)

        source_id = snapshot.get("source_id") or payload.get("source_id")
        entity_name = snapshot.get("entity") or payload.get("entity") or source_id
        schema = snapshot.get("schema") or {}
        fields = schema.get("fields", [])
        version_meta = schema.get("version_meta", {})

        # minimal validation
        if not isinstance(fields, list):
            raise ValueError("snapshot.schema.fields must be a list")

        # create a new snapshot id
        snapshot_id = str(uuid.uuid4())
        timestamp = version_meta.get("timestamp") or iso_now()
        created_by = version_meta.get("created_by") or "unknown"
        source_path = version_meta.get("source_path") or version_meta.get("path") or None

        logger.info("Persisting snapshot snapshot_id=%s entity=%s fields=%d", snapshot_id, entity_name, len(fields))

        # perform DB write in transaction
        session_kwargs = {"database": NEO4J_DB} if NEO4J_DB else {}
        try:
            with self._driver.session(**session_kwargs) as session:
                result = session.execute_write(
                    self._tx_persist_snapshot,
                    snapshot_id,
                    source_id,
                    entity_name,
                    fields,
                    created_by,
                    timestamp,
                    source_path,
                )
        except Neo4jError as e:
            logger.exception("Neo4j error while persisting snapshot: %s", str(e))
            raise

        response = {
            "request_id": request_id,
            "snapshot_id": snapshot_id,
            "previous_snapshot_id": result.get("previous_snapshot_id"),
            "impacted_jobs": result.get("impacted_jobs", []),
            "stored": True,
            "created_by": created_by,
            "timestamp": timestamp,
        }
        logger.info("Persisted snapshot snapshot_id=%s entity=%s impacted_jobs=%d", snapshot_id, entity_name, len(response["impacted_jobs"]))
        return response

    @staticmethod
    def _tx_persist_snapshot(tx,
                             snapshot_id: str,
                             source_id: Optional[str],
                             entity_name: str,
                             fields: List[Dict[str, Any]],
                             created_by: str,
                             timestamp: str,
                             source_path: Optional[str]) -> Dict[str, Any]:
        """
        Transaction body (executed inside a single DB transaction):
         1) Ensure Entity node exists (MERGE by name)
         2) Identify previous snapshot id for this entity (if any)
         3) Create Snapshot node
         4) Bulk-create SnapshotField nodes using UNWIND and link them to the Snapshot (bulk, safe)
         5) Create HAS_ENTITY relationship and OPTIONAL PREVIOUS_SNAPSHOT relationship
         6) Find impacted ETLJob nodes and return summary
        """

        # 1) Ensure entity exists (create placeholder if missing)
        q_entity_merge = """
        MERGE (e:Entity {name: $entity_name})
          ON CREATE SET e.type = coalesce($entity_type, 'file'), e.created = datetime()
          ON MATCH SET e.last_seen = datetime()
        RETURN id(e) AS entity_node_id, e.name AS entity_name
        """
        rec = tx.run(q_entity_merge, entity_name=entity_name, entity_type="file")
        row = rec.single()
        if not row:
            raise RuntimeError("Failed to MERGE Entity node")
        entity_node_id = row["entity_node_id"]

        # 2) Identify previous snapshot for this entity if any (most recent by timestamp)
        q_prev = """
        MATCH (s:Snapshot)-[:HAS_ENTITY]->(e:Entity {name: $entity_name})
        RETURN s.id AS snapshot_id, s.timestamp AS ts
        ORDER BY s.timestamp DESC
        LIMIT 1
        """
        prev = tx.run(q_prev, entity_name=entity_name)
        prev_row = prev.single()
        previous_snapshot_id = prev_row["snapshot_id"] if prev_row else None

        # 3) Create Snapshot node (timestamp is ISO string -> datetime($timestamp))
        q_create_snapshot = """
        CREATE (snap:Snapshot {
            id: $snapshot_id,
            source_id: $source_id,
            entity: $entity_name,
            timestamp: datetime($timestamp),
            created_by: $created_by,
            source_path: $source_path
        })
        RETURN id(snap) AS snap_node_id
        """
        snap_rec = tx.run(
            q_create_snapshot,
            snapshot_id=snapshot_id,
            source_id=source_id,
            entity_name=entity_name,
            timestamp=timestamp,
            created_by=created_by,
            source_path=source_path,
        )
        snap_row = snap_rec.single()
        if not snap_row:
            raise RuntimeError("Failed to create Snapshot node")
        snap_node_id = snap_row["snap_node_id"]

        # 4) Bulk-create SnapshotField nodes and link them to the Snapshot using UNWIND
        # Prepare field list for UNWIND with JSON serialization for hints (hints_json)
        field_list = []
        for f in fields:
            # ensure canonical shape/typing
            fname = f.get("name")
            ftype = f.get("type")
            fnullable = bool(f.get("nullable")) if "nullable" in f else True
            forder = int(f.get("ordinal", 0)) if f.get("ordinal") is not None else 0
            fhints = f.get("hints", {}) or {}
            fhints_json = json.dumps(fhints) if fhints else None
            field_list.append({
                "field_uuid": str(uuid.uuid4()),
                "name": fname,
                "data_type": ftype,
                "nullable": fnullable,
                "ordinal": forder,
                "hints_json": fhints_json,
            })

        fields_created = 0
        if field_list:
            # UNWIND bulk insertion - MATCH the snapshot by id and create all fields linked to it
            q_bulk = """
            MATCH (snap:Snapshot {id:$snapshot_id})
            UNWIND $field_list AS f
            CREATE (sf:SnapshotField {
                id: f.field_uuid,
                name: f.name,
                data_type: f.data_type,
                nullable: f.nullable,
                ordinal: f.ordinal,
                hints_json: f.hints_json
            })
            CREATE (snap)-[:HAS_FIELD_COPY]->(sf)
            RETURN count(sf) AS created_count
            """
            rec_bulk = tx.run(q_bulk, snapshot_id=snapshot_id, field_list=field_list)
            rrow = rec_bulk.single()
            if rrow:
                fields_created = int(rrow["created_count"]) if rrow.get("created_count") is not None else 0
            else:
                fields_created = 0

        # 5) Link Snapshot -> Entity and Snapshot -> previous snapshot if exists
        tx.run(
            "MATCH (snap:Snapshot {id:$snapshot_id}), (e:Entity {name:$entity_name}) MERGE (snap)-[:HAS_ENTITY]->(e)",
            snapshot_id=snapshot_id,
            entity_name=entity_name,
        )
        if previous_snapshot_id:
            tx.run(
                "MATCH (snap:Snapshot {id:$snapshot_id}), (prev:Snapshot {id:$previous_snapshot_id}) MERGE (snap)-[:PREVIOUS_SNAPSHOT]->(prev)",
                snapshot_id=snapshot_id,
                previous_snapshot_id=previous_snapshot_id,
            )

        # 6) Find impacted ETL jobs that use or produce this Entity (distinct)
        q_jobs = """
        MATCH (j:ETLJob)-[:USES_SOURCE|:PRODUCES]->(e:Entity {name: $entity_name})
        RETURN DISTINCT j.job_id AS job_id
        """
        jobs_cursor = tx.run(q_jobs, entity_name=entity_name)
        impacted_jobs = [r["job_id"] for r in jobs_cursor]

        return {
            "previous_snapshot_id": previous_snapshot_id,
            "impacted_jobs": impacted_jobs,
            "fields_created": fields_created,
            "entity_node_id": entity_node_id,
            "snapshot_node_id": snap_node_id,
        }


if __name__ == "__main__":
    import json, sys

    sample_payload = {
        "request_id": "local-test-3",
        "source_id": "people-info.csv",
        "entity": "people-info.csv",
        "snapshot": {
            "source_id": "people-info.csv",
            "entity": "people-info.csv",
            "schema": {
                "fields": [
                    {"name": "name", "type": "string", "nullable": False, "ordinal": 0},
                    {"name": "date_of_birth", "type": "date", "nullable": False, "ordinal": 1},
                    {"name": "gender", "type": "string", "nullable": True, "ordinal": 2},
                    {"name": "company", "type": "string", "nullable": True, "ordinal": 3},
                    {"name": "designation", "type": "string", "nullable": True, "ordinal": 4}
                ],
                "version_meta": {
                    "created_by": "schema_drift_detector_agent",
                    "timestamp": "2025-11-19T13:03:31.745777+00:00",
                    "source_path": "../schema_drift_detector/examples/people-info.csv",
                },
            },
        },
    }

    agent = MetadataAgent()
    try:
        out = agent.persist_snapshot(sample_payload)
        print(json.dumps(out, indent=2))
    except Exception as e:
        print("ERROR:", str(e), file=sys.stderr)
        raise
    finally:
        agent.close()
