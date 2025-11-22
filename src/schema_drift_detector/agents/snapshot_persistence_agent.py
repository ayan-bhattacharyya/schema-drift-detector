# src/schema_drift/agents/snapshot_persistence_agent.py
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

logger = logging.getLogger("snapshot_persistence_agent")
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


class SnapshotPersistenceAgent:
    """
    Snapshot persistence agent persists canonical snapshots into Neo4j and manages version links.
    Updated to work with new schema: Snapshot nodes with HAS_FIELD relationships, IntegrationCatalog instead of ETLJob.
    """

    def __init__(self) -> None:
        logger.info("Connecting to Neo4j at %s", NEO4J_URI)
        self._driver = _driver()

    def close(self) -> None:
        try:
            self._driver.close()
        except Exception:
            logger.exception("Error closing Neo4j driver")

    def persist_snapshot(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Persist snapshot payload and return metadata including snapshot id and impacted pipelines.

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
            "impacted_pipelines": result.get("impacted_pipelines", []),
            "stored": True,
            "created_by": created_by,
            "timestamp": timestamp,
        }
        logger.info("Persisted snapshot snapshot_id=%s entity=%s impacted_pipelines=%d", 
                   snapshot_id, entity_name, len(response["impacted_pipelines"]))
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
         1) Identify previous snapshot id for this component (if any)
         2) Create Snapshot node
         3) Bulk-create SnapshotField nodes using UNWIND and link them to the Snapshot
         4) Create PREVIOUS_SNAPSHOT relationship if applicable
         5) Find impacted IntegrationCatalog (pipelines) and return summary
        """

        # 1) Identify previous snapshot for this component if any (most recent by timestamp)
        # Match by component name (entity_name)
        q_prev = """
        MATCH (s:Snapshot {component: $component})
        RETURN s.id AS snapshot_id, s.timestamp AS ts
        ORDER BY s.timestamp DESC
        LIMIT 1
        """
        prev = tx.run(q_prev, component=entity_name)
        prev_row = prev.single()
        previous_snapshot_id = prev_row["snapshot_id"] if prev_row else None

        # 2) Create Snapshot node
        q_create_snapshot = """
        CREATE (snap:Snapshot {
            id: $snapshot_id,
            component: $component,
            source_path: $source_path,
            timestamp: datetime($timestamp),
            created_by: $created_by
        })
        RETURN id(snap) AS snap_node_id
        """
        snap_rec = tx.run(
            q_create_snapshot,
            snapshot_id=snapshot_id,
            component=entity_name,
            timestamp=timestamp,
            created_by=created_by,
            source_path=source_path,
        )
        snap_row = snap_rec.single()
        if not snap_row:
            raise RuntimeError("Failed to create Snapshot node")
        snap_node_id = snap_row["snap_node_id"]

        # 3) Bulk-create SnapshotField nodes and link them to the Snapshot using UNWIND
        field_list = []
        for f in fields:
            fname = f.get("name")
            ftype = f.get("type") or f.get("data_type")
            fnullable = bool(f.get("nullable")) if "nullable" in f else True
            forder = int(f.get("ordinal", 0)) if f.get("ordinal") is not None else 0
            fhints = f.get("hints", {}) or {}
            fhints_json = json.dumps(fhints) if fhints else None
            
            # Generate field ID based on snapshot_id, field name, and ordinal
            field_id = f"{snapshot_id}|{fname}|{forder}"
            
            field_list.append({
                "field_id": field_id,
                "name": fname,
                "data_type": ftype,
                "nullable": fnullable,
                "ordinal": forder,
                "hints_json": fhints_json,
            })

        fields_created = 0
        if field_list:
            # Use HAS_FIELD relationship (not HAS_FIELD_COPY) to match new schema
            q_bulk = """
            MATCH (snap:Snapshot {id:$snapshot_id})
            UNWIND $field_list AS f
            CREATE (sf:SnapshotField {
                id: f.field_id,
                name: f.name,
                data_type: f.data_type,
                nullable: f.nullable,
                ordinal: f.ordinal
            })
            CREATE (snap)-[:HAS_FIELD]->(sf)
            RETURN count(sf) AS created_count
            """
            rec_bulk = tx.run(q_bulk, snapshot_id=snapshot_id, field_list=field_list)
            rrow = rec_bulk.single()
            if rrow:
                fields_created = int(rrow["created_count"]) if rrow.get("created_count") is not None else 0

        # 4) Link to previous snapshot if exists
        if previous_snapshot_id:
            tx.run(
                "MATCH (snap:Snapshot {id:$snapshot_id}), (prev:Snapshot {id:$previous_snapshot_id}) "
                "MERGE (snap)-[:PREVIOUS_SNAPSHOT]->(prev)",
                snapshot_id=snapshot_id,
                previous_snapshot_id=previous_snapshot_id,
            )

        # 5) Find impacted IntegrationCatalog (pipelines) that cover this component
        q_pipelines = """
        MATCH (ic:IntegrationCatalog)-[:COVERS_COMPONENT]->(s:Snapshot {component: $component})
        RETURN DISTINCT ic.pipeline AS pipeline
        """
        pipelines_cursor = tx.run(q_pipelines, component=entity_name)
        impacted_pipelines = [r["pipeline"] for r in pipelines_cursor]

        return {
            "previous_snapshot_id": previous_snapshot_id,
            "impacted_pipelines": impacted_pipelines,
            "fields_created": fields_created,
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
                    "created_by": "snapshot_persistence_agent",
                    "timestamp": "2025-11-22T11:45:00.000000+00:00",
                    "source_path": "/Users/ayanbhattacharyya/Documents/ai-workspace/schema_drift_detector/schema_drift_detector/examples/people-info.csv",
                },
            },
        },
    }

    agent = SnapshotPersistenceAgent()
    try:
        out = agent.persist_snapshot(sample_payload)
        print(json.dumps(out, indent=2))
    except Exception as e:
        print("ERROR:", str(e), file=sys.stderr)
        raise
    finally:
        agent.close()
