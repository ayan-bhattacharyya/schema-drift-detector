import os
import logging
import datetime

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, ValidationError, field_validator
from neo4j import GraphDatabase
from dotenv import load_dotenv

logger = logging.getLogger("source_schema_identifier_agent")
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
NEO4J_DB  = get_env("NEO4J_DB", required=True)

def _driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def _serialize_value(v: Any):
    """
    Convert Neo4j/driver types to JSON-serializable Python primitives.
    - datetime/date/time/neo4j DateTime -> ISO string
    - neo4j.types.GraphNode / Node -> dict(properties) (driver usually returns basic types)
    - lists/dicts -> recursively convert
    - fallback: str(v)
    """
    # None / primitives
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v

    # datetime from Python's datetime module
    if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
        # use ISO format
        try:
            return v.isoformat()
        except Exception:
            return str(v)

    # Neo4j driver's DateTime / Temporal types often implement isoformat or to_native()
    # We check common methods defensively:
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except Exception:
            pass
    if hasattr(v, "to_native"):
        try:
            native = v.to_native()
            if isinstance(native, (datetime.date, datetime.datetime, datetime.time)):
                return native.isoformat()
            return native
        except Exception:
            pass

    # If it's a mapping/dict-like, serialize items
    if isinstance(v, dict):
        return {str(k): _serialize_value(val) for k, val in v.items()}

    # If it's a list/tuple, serialize elements
    if isinstance(v, (list, tuple, set)):
        return [_serialize_value(x) for x in v]

    # fallback: try properties attribute (GraphNode) or str()
    if hasattr(v, "items"):
        try:
            return {str(k): _serialize_value(val) for k, val in v.items()}
        except Exception:
            pass
    # Best effort
    try:
        return str(v)
    except Exception:
        return None


# -------------------------
# Input model
# -------------------------
class IdentifierInput(BaseModel):
    pipeline: str
    request_id: Optional[str] = None

    @field_validator("pipeline")
    @classmethod
    def pipeline_not_empty(cls, v):
        if not v or not isinstance(v, str) or not v.strip():
            raise ValueError("pipeline must be a non-empty string")
        return v.strip()

# -------------------------
# Helper queries
# -------------------------
# - Get ETL job node and policy props
JOB_QUERY = """
MATCH (j:ETLJob {job_id:$job_id})
RETURN elementId(j) AS element_id, j.job_id AS job_id, j.name AS name,
       j.auto_heal_allowed AS auto_heal_allowed,
       j.notify_on_breaking AS notify_on_breaking,
       j.notify_channels AS notify_channels,
       j.operator_contact AS operator_contact,
       properties(j) AS props
LIMIT 1
"""

# - Get source Entities used by job
SOURCES_QUERY = """
MATCH (j:ETLJob {job_id:$job_id})-[:USES_SOURCE]->(s:Entity)
RETURN elementId(s) AS element_id, s.name AS name, s.type AS type, s.source_path AS source_path, properties(s) AS props
ORDER BY s.name
"""

# - Get produced/target Entities
PRODUCES_QUERY = """
MATCH (j:ETLJob {job_id:$job_id})-[:PRODUCES]->(p:Entity)
RETURN elementId(p) AS element_id, p.name AS name, p.type AS type, p.source_path AS source_path, properties(p) AS props
ORDER BY p.name
"""

# - Get fields for an entity by name
FIELDS_QUERY = """
MATCH (e:Entity {name:$entity_name})-[:HAS_FIELD]->(f:Field)
RETURN elementId(f) AS element_id, f.field_id AS field_id, f.name AS name, f.data_type AS data_type, f.nullable AS nullable, f.ordinal AS ordinal, properties(f) AS props
ORDER BY f.ordinal
"""

# - Get transformations for the job
TRANSFORMS_QUERY = """
MATCH (t:Transformation)-[:APPLIES_TO_JOB]->(j:ETLJob {job_id:$job_id})
RETURN t.transformation_id AS transformation_id, t.mapping_order AS mapping_order,
       t.source_field AS source_field, t.target_field AS target_field, t.expression AS expression, t.description AS description,
       properties(t) AS props
ORDER BY t.mapping_order
"""

# Optional: get mappings via MAPPED_TO relationships (if you prefer)
MAPPED_TO_QUERY = """
MATCH (s:Entity)-[:HAS_FIELD]->(sf:Field)-[m:MAPPED_TO {job_id:$job_id}]->(tf:Field)
RETURN id(sf) AS src_field_node_id, sf.field_id AS src_field_id, sf.name AS src_name,
       id(tf) AS target_field_node_id, tf.field_id AS target_field_id, tf.name AS target_name,
       m.mapping_order AS mapping_order, m.expression AS expression, m
ORDER BY m.mapping_order
"""
# -------------------------
# Agent implementation
# -------------------------
class SourceSchemaIdentifierAgent:
    """
    Agent that, given a pipeline name (ETL job id), queries Neo4j metadata/catalog
    to resolve sources, produced targets, fields, transformations, and job policies.
    """

    def __init__(self):
        self._driver = _driver()

    def _run_read(self, cypher: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        params = params or {}
        results = []
        # use default database unless NEO4J_DB set
        if not NEO4J_DB:
            raise ValueError("NEO4J_DB environment variable is not set.")
        db = NEO4J_DB
        with self._driver.session(database=db) as session:
            res = session.run(cypher, params)
            for record in res:
                row = {}
                for k in record.keys():
                    row[k] = _serialize_value(record[k])
                results.append(row)
        return results

    def _fetch_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        rows = self._run_read(JOB_QUERY, {"job_id": job_id})
        return rows[0] if rows else None

    def _fetch_sources(self, job_id: str) -> List[Dict[str, Any]]:
        return self._run_read(SOURCES_QUERY, {"job_id": job_id})

    def _fetch_produces(self, job_id: str) -> List[Dict[str, Any]]:
        return self._run_read(PRODUCES_QUERY, {"job_id": job_id})

    def _fetch_fields(self, entity_name: str) -> List[Dict[str, Any]]:
        return self._run_read(FIELDS_QUERY, {"entity_name": entity_name})

    def _fetch_transforms(self, job_id: str) -> List[Dict[str, Any]]:
        return self._run_read(TRANSFORMS_QUERY, {"job_id": job_id})

    def run(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Synchronous run method (Crew call_agent supports sync agents).
        Expected inputs: { "request_id": "<uuid>", "pipeline": "<pipeline_name_or_id>" }
        Returns:
          {
            "pipeline": "<pipeline>",
            "sources": [ { source metadata, fields: [...] }, ... ],
            "produces": [ { entity: "...", fields: [...] }, ... ],
            "transformations": [ ... ],
            "policies": { ... }
          }
        """
        try:
            iv = IdentifierInput(**inputs)
            pipeline = iv.pipeline
            request_id = iv.request_id

            logger.info("Resolving pipeline metadata for pipeline=%s request_id=%s", pipeline, request_id)

            job = self._fetch_job(pipeline)
            if not job:
                msg = f"ETLJob with job_id='{pipeline}' not found in metadata store"
                logger.warning(msg)
                return {"pipeline": pipeline, "sources": [], "produces": [], "transformations": [], "policies": {} , "error": msg}

            # Policies: read from job properties, default safe values
            props = job.get("props") or {}
            policies = {
                "auto_heal_allowed": bool(props.get("auto_heal_allowed", False)),
                "notify_on_breaking": bool(props.get("notify_on_breaking", True)),
                "notify_channels": props.get("notify_channels", ["email"]),
                "operator_contact": props.get("operator_contact", None)
            }

            # Sources
            raw_sources = self._fetch_sources(pipeline)
            sources = []
            for s in raw_sources:
                entity_name = s.get("name")
                # fetch fields metadata for entity
                fields_raw = self._fetch_fields(entity_name)
                fields = []
                for f in fields_raw:
                    fields.append({
                        "field_id": f.get("field_id"),
                        "name": f.get("name"),
                        "type": f.get("data_type"),
                        "nullable": f.get("nullable"),
                        "ordinal": f.get("ordinal"),
                    })
                sources.append({
                    "source_id": entity_name,
                    "type": (s.get("type") or "file"),
                    "entity": entity_name,
                    "metadata_ref": { "node_id": s.get("node_id"), "properties": s.get("props", {}) },
                    "fields": fields
                })

            # Produces (targets)
            raw_produces = self._fetch_produces(pipeline)
            produces = []
            for p in raw_produces:
                entity_name = p.get("name")
                fields_raw = self._fetch_fields(entity_name)
                fields = []
                for f in fields_raw:
                    fields.append({
                        "field_id": f.get("field_id"),
                        "name": f.get("name"),
                        "type": f.get("data_type"),
                        "nullable": f.get("nullable"),
                        "ordinal": f.get("ordinal"),
                    })
                produces.append({
                    "entity": entity_name,
                    "metadata_ref": { "node_id": p.get("node_id"), "properties": p.get("props", {}) },
                    "fields": fields
                })

            # Transformations
            raw_transforms = self._fetch_transforms(pipeline)
            transformations = []
            for t in raw_transforms:
                transformations.append({
                    "transformation_id": t.get("transformation_id"),
                    "mapping_order": t.get("mapping_order"),
                    "source_field": t.get("source_field"),
                    "target_field": t.get("target_field"),
                    "expression": t.get("expression"),
                    "description": t.get("description"),
                    "metadata": t.get("props", {})
                })

            result = {
                "pipeline": pipeline,
                "sources": sources,
                "produces": produces,
                "transformations": transformations,
                "policies": policies,
                "job_node": { "node_id": job.get("node_id"), "properties": job.get("props", {}) },
                "request_id": request_id
            }

            logger.info("Resolved metadata for pipeline=%s sources=%d produces=%d transforms=%d",
                        pipeline, len(sources), len(produces), len(transformations))
            return result

        except ValidationError as ve:
            logger.error("Invalid input: %s", ve)
            return {"error": "invalid_input", "details": ve.errors()}
        except Exception as e:
            logger.exception("Failed to resolve pipeline metadata: %s", str(e))
            return {"error": "internal_error", "message": str(e)}


# Optional: allow running standalone for local testing
if __name__ == "__main__":
    import json
    agent = SourceSchemaIdentifierAgent()
    sample_input = {"pipeline": "CRM-To-Finance-PeopleData", "request_id": "local-test-1"}
    out = agent.run(sample_input)
    print(json.dumps(out, indent=2))