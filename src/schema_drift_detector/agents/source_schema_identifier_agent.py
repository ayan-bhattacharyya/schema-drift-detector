import os
import logging
import datetime

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, ValidationError, field_validator
from neo4j import GraphDatabase
from dotenv import load_dotenv

try:
    from neo4j.exceptions import Neo4jError
except Exception:  # pragma: no cover - defensive fallback
    Neo4jError = Exception

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
# Helper queries for new schema
# -------------------------
# - Get IntegrationCatalog by pipeline name
INTEGRATION_CATALOG_QUERY = """
MATCH (ic:IntegrationCatalog {pipeline: $pipeline})
RETURN elementId(ic) AS element_id, properties(ic) AS props
LIMIT 1
"""

# - Get HealingPolicy and linked HealingStrategy
HEALING_POLICY_QUERY = """
MATCH (ic:IntegrationCatalog {pipeline: $pipeline})-[:HAS_HEALING_POLICY]->(hp:HealingPolicy)
OPTIONAL MATCH (hp)-[:USES_HEALING_STRATEGY]->(hs:HealingStrategy)
RETURN properties(hp) AS healing_policy, properties(hs) AS healing_strategy
LIMIT 1
"""

# - Get NotificationPolicy with all details
NOTIFICATION_POLICY_QUERY = """
MATCH (ic:IntegrationCatalog {pipeline: $pipeline})-[:HAS_NOTIFICATION_POLICY]->(np:NotificationPolicy)
RETURN properties(np) AS notification_policy
LIMIT 1
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
    
    def close(self) -> None:
        try:
            self._driver.close()
        except Exception:
            logger.exception("Error closing Neo4j driver")

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

    def _fetch_integration_catalog(self, pipeline: str) -> Optional[Dict[str, Any]]:
        """Fetch IntegrationCatalog node for the given pipeline."""
        rows = self._run_read(INTEGRATION_CATALOG_QUERY, {"pipeline": pipeline})
        return rows[0] if rows else None

    def _fetch_healing_policy(self, pipeline: str) -> Dict[str, Any]:
        """Fetch HealingPolicy and linked HealingStrategy for the given pipeline."""
        rows = self._run_read(HEALING_POLICY_QUERY, {"pipeline": pipeline})
        if rows:
            return {
                "healing_policy": rows[0].get("healing_policy"),
                "healing_strategy": rows[0].get("healing_strategy")
            }
        return {"healing_policy": None, "healing_strategy": None}

    def _fetch_notification_policy(self, pipeline: str) -> Optional[Dict[str, Any]]:
        """Fetch NotificationPolicy for the given pipeline."""
        rows = self._run_read(NOTIFICATION_POLICY_QUERY, {"pipeline": pipeline})
        return rows[0].get("notification_policy") if rows else None

    def run(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Synchronous run method (Crew call_agent supports sync agents).
        Expected inputs: { "request_id": "<uuid>", "pipeline": "<pipeline_name>" }
        Returns:
          {
            "pipeline": "<pipeline>",
            "integration_catalog": { ... },
            "healing_policy": { ... },
            "healing_strategy": { ... },
            "notification_policy": { ... },
            "request_id": "..."
          }
        """
        try:
            iv = IdentifierInput(**inputs)
            pipeline = iv.pipeline
            request_id = iv.request_id

            logger.info("Resolving pipeline metadata for pipeline=%s request_id=%s", pipeline, request_id)

            # Fetch IntegrationCatalog
            ic = self._fetch_integration_catalog(pipeline)
            if not ic:
                msg = f"IntegrationCatalog with pipeline='{pipeline}' not found in metadata store"
                logger.warning(msg)
                return {
                    "pipeline": pipeline,
                    "integration_catalog": None,
                    "healing_policy": None,
                    "healing_strategy": None,
                    "notification_policy": None,
                    "error": msg,
                    "request_id": request_id
                }

            # Extract integration catalog properties
            ic_props = ic.get("props") or {}
            integration_catalog = {
                "source_system": ic_props.get("source_system"),
                "target_system": ic_props.get("target_system"),
                "integration_type": ic_props.get("integration_type"),
                "source_type": ic_props.get("source_type"),
                "target_type": ic_props.get("target_type"),
                "source_component": ic_props.get("source_component"),
                "target_component": ic_props.get("target_component"),
                "created": ic_props.get("created"),
                "last_seen": ic_props.get("last_seen")
            }

            # Fetch HealingPolicy and HealingStrategy
            healing_data = self._fetch_healing_policy(pipeline)
            healing_policy = healing_data.get("healing_policy")
            healing_strategy = healing_data.get("healing_strategy")

            # Fetch NotificationPolicy
            notification_policy = self._fetch_notification_policy(pipeline)

            result = {
                "pipeline": pipeline,
                "integration_catalog": integration_catalog,
                "healing_policy": healing_policy,
                "healing_strategy": healing_strategy,
                "notification_policy": notification_policy,
                "request_id": request_id
            }

            logger.info("Resolved metadata for pipeline=%s, healing_policy=%s, notification_policy=%s",
                        pipeline, bool(healing_policy), bool(notification_policy))
            return result

        except ValidationError as ve:
            logger.error("Invalid input: %s", ve)
            return {"error": "invalid_input", "details": ve.errors()}
        except Exception as e:
            logger.exception("Failed to resolve pipeline metadata: %s", str(e))
            return {"error": "internal_error", "message": str(e)}


# Optional: allow running standalone for local testing
if __name__ == "__main__":
    import json, sys
    agent = SourceSchemaIdentifierAgent()
    try:
        sample_input = {"pipeline": "CRM-To-Finance-PeopleData", "request_id": "local-test-2"}
        out = agent.run(sample_input)
        print(json.dumps(out, indent=2))
    except Exception as e:
        print("ERROR:", str(e), file=sys.stderr)
        raise
    finally:
        agent.close()
    