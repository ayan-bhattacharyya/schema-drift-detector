from __future__ import annotations

import json
import logging
import uuid
import os

from typing import Any, Dict, Optional
from dotenv import load_dotenv
from schema_drift_detector.agents.source_schema_identifier_agent import SourceSchemaIdentifierAgent
from schema_drift_detector.agents.csv_crawler_agent import CSVCrawlerAgent
from schema_drift_detector.agents.metadata_agent import MetadataAgent
from schema_drift_detector.agents.detector_agent import DetectorAgent


logger = logging.getLogger("orchestrator_agent")
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

USE_LLM = get_env("USE_GEMINI", required=True)


def new_request_id() -> str:
    return str(uuid.uuid4())

def call_source_schema_identifier_agent(pipeline: str, request_id: str) -> Dict[str, Any]:
    """
    Instantiate source schema identifier agent locally and call run().
    Returns JSON/dict output from the agent.
    """
    agent = None
    try:
        agent = SourceSchemaIdentifierAgent()
        # Build input exactly as your agent expects
        in_payload = {"pipeline": pipeline, "request_id": request_id}
        logger.info("Calling SourceSchemaIdentifierAgent with request_id=%s pipeline=%s", request_id, pipeline)
        logger.debug("SourceSchemaIdentifierAgent input: %s", json.dumps(in_payload))
        resp = agent.run(in_payload)
        logger.debug("SourceSchemaIdentifierAgent RAW response: %s", json.dumps(resp, indent=2))
        return resp
    finally:
        if agent:
            try:
                agent.close()
            except Exception:
                logger.exception("Error closing SourceSchemaIdentifierAgent")

def call_csv_crawler_agent(crawl_input: Dict[str, Any]) -> Dict[str, Any]:
    """
    Instantiate CSV crawler agent locally and call run().
    crawl_input must match the structure you provided.
    """

    agent = CSVCrawlerAgent()
    logger.info("Calling CSVCrawlerAgent request_id=%s source_id=%s", crawl_input.get("request_id"), crawl_input.get("source_id"))
    logger.debug("CSVCrawlerAgent input: %s", json.dumps(crawl_input, indent=2))
    resp = agent.run(crawl_input)
    logger.debug("CSVCrawlerAgent RAW response: %s", json.dumps(resp, indent=2))
    return resp

def call_metadata_agent(sample_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Modular wrapper to call MetadataAgent.persist_snapshot.
    Ensures the agent is created and closed cleanly.
    Returns the metadata agent response (dict).
    """
    agent = None
    try:
        agent = MetadataAgent()
        logger.info("Calling MetadataAgent.persist_snapshot request_id=%s source_id=%s", sample_payload.get("request_id"), sample_payload.get("source_id"))
        logger.debug("MetadataAgent input: %s", json.dumps(sample_payload, indent=2))
        resp = agent.persist_snapshot(sample_payload)
        logger.debug("MetadataAgent RAW response: %s", json.dumps(resp, indent=2))
        return resp
    finally:
        if agent:
            try:
                agent.close()
            except Exception:
                logger.exception("Error closing MetadataAgent")

def call_detector_agent(detector_input: Dict[str, Any]) -> Dict[str, Any]:
    """
    Call DetectorAgent.run() and return result.
    """
    agent = None
    try:
        agent = DetectorAgent()
        logger.debug("DetectorAgent input: %s", json.dumps(detector_input, indent=2))
        resp = agent.run(detector_input)
        logger.debug("DetectorAgent RAW response: %s", json.dumps(resp, indent=2))
        return resp
    finally:
        if agent:
            try:
                agent.close()
            except Exception:
                logger.exception("Error closing DetectorAgent")


def orchestrate_pipeline(pipeline: str) -> Dict[str, Any]:
    """
    Orchestrate pipeline:
      1. Generate request_id
      2. Call source schema identifier agent
      3. Parse first source entry and choose crawler
      4. Call CSV crawler if file-like
      5. Parse the field/schema info from crawler output
      6. Build metadata snapshot input
      7. Call metadata agent to persist snapshot and get the current and previous snapshot IDs
      8. validate metadata persisted
      9. Call detector agent to check for schema drift
     10. Validate detector output
     11. Build and return orchestration output object
    """
    request_id = new_request_id()
    logger.info("Orchestrator starting pipeline=%s request_id=%s", pipeline, request_id)

    # Get source schema info
    schema_resp = call_source_schema_identifier_agent(pipeline, request_id)
    if not isinstance(schema_resp, dict):
        raise RuntimeError("Source schema identifier returned non-dict response")

    # debug raw
    logger.debug("Schema identifier response: %s", json.dumps(schema_resp, indent=2))

    # Parse sources / take first source (extend as needed)
    sources = schema_resp.get("sources") or []
    if not sources:
        raise RuntimeError(f"No 'sources' returned by source schema identifier for pipeline={pipeline}")

    src = sources[0]
    source_id = src.get("source_id") or src.get("entity")
    src_type = (src.get("type") or "").lower()
    entity = src.get("entity") or source_id

    # metadata_ref -> properties -> source_path
    metadata_ref = src.get("metadata_ref") or {}
    properties = metadata_ref.get("properties") or {}
    source_path = properties.get("source_path")

    transformations = schema_resp.get("transformations") or []
    policies = schema_resp.get("policies") or {}

    logger.debug("Parsed source: source_id=%s type=%s entity=%s source_path=%s", source_id, src_type, entity, source_path)
    logger.debug("Transformations: %s", json.dumps(transformations, indent=2))
    logger.debug("Policies: %s", json.dumps(policies, indent=2))

    # Decide crawler - treat 'file'/'csv'/'text' as CSV/file for now
    file_like = {"file", "csv", "text"}
    crawler_result = None
    if src_type in file_like:
        # Build crawl_input using the exact shape you provided as truth
        crawl_input = {
            "request_id": request_id,
            "source_id": source_id,
            "entity": entity,
            "metadata_ref": {"properties": {"source_path": source_path}} if source_path else src.get("metadata_ref", {}),
            "options": {"header_rows": 1}
        }
        # Call crawler (only returns crawler result now)
        crawler_result = call_csv_crawler_agent(crawl_input)
    else:
        raise NotImplementedError(f"Unsupported source type '{src_type}' — only 'file/csv/text' supported by this orchestrator")
    
    # Prepare metadata payload from crawler_result and call metadata agent
    snapshot = crawler_result.get("snapshot") if isinstance(crawler_result, dict) else None
    schema = snapshot.get("schema") if snapshot and isinstance(snapshot, dict) else {}
    fields = schema.get("fields", []) if isinstance(schema, dict) else []
    version_meta = schema.get("version_meta", {}) if isinstance(schema, dict) else {}

    # canonicalize fields for metadata agent
    canonical_fields = []
    for f in fields:
        fname = f.get("name")
        ftype = f.get("type") or f.get("data_type")
        fnullable = bool(f.get("nullable")) if "nullable" in f else True
        forder = int(f.get("ordinal", 0)) if f.get("ordinal") is not None else 0
        fhints = f.get("hints", {}) or f.get("hints_json", {}) or {}
        canonical_fields.append({
            "name": fname,
            "type": ftype,
            "nullable": fnullable,
            "ordinal": forder,
            "hints": fhints
        })
    
    # Build metadata payload expected by MetadataAgent.persist_snapshot
    metadata_snapshot_input = {
        "request_id": request_id,
        "source_id": source_id,
        "entity": entity,
        "snapshot": {
            "source_id": source_id,
            "entity": entity,
            "schema": {
                "fields": canonical_fields,
                "version_meta": {
                    "created_by": version_meta.get("created_by") or "csv_crawler_agent",
                    "timestamp": version_meta.get("timestamp") or None,
                    "source_path": version_meta.get("source_path") or source_path
                }
            }
        }
    }

    # Remove explicit None timestamp so metadata agent can default it
    if metadata_snapshot_input["snapshot"]["schema"]["version_meta"].get("timestamp") is None:
        metadata_snapshot_input["snapshot"]["schema"]["version_meta"].pop("timestamp", None)
    
    # call metadata agent to persist snapshot
    try:
        metadata_snapshot_result = call_metadata_agent(metadata_snapshot_input)
    except Exception:
        logger.exception("Snapshopt Persistence agent call failed for request_id=%s source_id=%s", request_id, source_id)
        raise

    # Validate metadata stored flag
        if not isinstance(metadata_store_result, dict) or not metadata_store_result.get("stored"):
            logger.error("Metadata store returned stored=%s (full result: %s)", metadata_store_result.get("stored") if isinstance(metadata_store_result, dict) else None, json.dumps(metadata_store_result))
            raise RuntimeError("MetadataAgent failed to store snapshot (stored flag false)")
    
    # Call detector agent to check for schema drift
    detector_input = {
        "request_id": request_id,
        "snapshot_id": metadata_snapshot_result.get("current_snapshot_id"),
        "previous_snapshot_id": metadata_snapshot_result.get("previous_snapshot_id"),
        "pipeline": pipeline,
        "options": {"use_llm": USE_LLM.lower()}
    }
    detector_result = call_detector_agent(detector_input)

    # Build output result object
    orchestration_output = {
        "request_id": request_id,
        "pipeline": pipeline,
        "source": {
            "source_id": source_id,
            "type": src_type,
            "entity": entity,
            "source_path": source_path,
            "fields_from_identifier": src.get("fields", []),
        },
        "transformations": transformations,
        "policies": policies,
        "crawler_result": crawler_result,
        "metadata_snapshot_result": metadata_snapshot_result,
        "detector_result": detector_result
    }

    logger.info("Orchestration finished pipeline=%s request_id=%s", pipeline, request_id)
    logger.info("Orchestration output: %s", json.dumps(orchestration_output, indent=2))
    return orchestration_output

class OrchestratorAgent:
    """
    OrchestratorAgent class to encapsulate orchestration logic.
    """

    def __init__(self):
        logger.info("OrchestratorAgent initialised")
        self.schema_agent = SourceSchemaIdentifierAgent()
        self.csv_agent = CSVCrawlerAgent()

    def run(self, pipeline: str, source_location: Optional[str] = None) -> Dict[str, Any]:
        return orchestrate_pipeline(pipeline)

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    orch = OrchestratorAgent()
    PIPELINE = "CRM-To-Finance-PeopleData"
    result = orch.run(PIPELINE)


