import os
import uuid
import logging
import importlib
import json

from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from dataclasses import dataclass

try:
    from schema_drift_detector.agents.source_schema_identifier_agent import SourceSchemaIdentifierAgent
    from schema_drift_detector.agents.csv_crawler_agent import CSVCrawlerAgent
    from schema_drift_detector.agents.metadata_agent import MetadataAgent
    from schema_drift_detector.agents.detector_agent import DetectorAgent

except ImportError:
    SourceSchemaIdentifierAgent = None
    CSVCrawlerAgent = None
    MetadataAgent = None
    DetectorAgent = None

logger = logging.getLogger("orchestrator_agent")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

@dataclass
class OrchestratorAgent:
    """Orchestrator that drives the crawl -> persist -> detect -> heal workflow for a pipeline.


    This implementation calls local agent classes directly. In production you might instead call
    remote microservices, queue messages, or invoke CrewAI task runners.
    """


    # optional agent instances can be passed for dependency injection / testing
    source_identifier: Optional[Any] = None
    csv_crawler: Optional[Any] = None
    metadata_store: Optional[Any] = None
    detector: Optional[Any] = None

    def __post_init__(self):
        # lazy-instantiate agents if not injected
        if self.source_identifier is None and SourceSchemaIdentifierAgent is not None:
            self.source_identifier = SourceSchemaIdentifierAgent()
        if self.csv_crawler is None and CSVCrawlerAgent is not None:
            self.csv_crawler = CSVCrawlerAgent()
        if self.metadata_store is None and MetadataAgent is not None:
            self.metadata_store = MetadataAgent()
        if self.detector is None and DetectorAgent is not None:
            self.detector = DetectorAgent()

    def _gen_request_id(self) -> str:
        return str(uuid.uuid4())

# -------------------------
# Simple guard to prevent processing sample rows / PII
# -------------------------
FORBIDDEN_SNAPSHOT_KEYS = {"sample_rows", "samples", "row_sample", "data", "rows", "example_values", "example"}
def assert_schema_only_snapshot(snapshot: Dict[str, Any]) -> None:
    """
    Raise ValueError if snapshot contains forbidden keys (sample data / rows).
    It scans recursively.
    """
    def _contains_forbidden(o: Any) -> bool:
        if isinstance(o, dict):
            for k, v in o.items():
                if k in FORBIDDEN_SNAPSHOT_KEYS:
                    return True
                if _contains_forbidden(v):
                    return True
        elif isinstance(o, (list, tuple, set)):
            for item in o:
                if _contains_forbidden(item):
                    return True
        return False

    if _contains_forbidden(snapshot):
        raise ValueError("Snapshot contains forbidden sample data or PII keys.")

async def _call_agent_http(agent_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = CREW_HTTP_BRIDGE_URL.rstrip("/") + f"/agents/{agent_name}/run"
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        return r.json()

def _call_agent_local(module_path: str, class_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Local import invocation: imports module and calls .run(payload) synchronously.
    Supports sync or async run() on the agent class.
    """
    module = importlib.import_module(module_path)
    AgentCls = getattr(module, class_name)
    agent_instance = AgentCls()
    run_fn = getattr(agent_instance, "run", None)
    if run_fn is None:
        raise RuntimeError(f"Agent {module_path}.{class_name} has no run() method")
    import asyncio, inspect
    if inspect.iscoroutinefunction(run_fn):
        return asyncio.get_event_loop().run_until_complete(run_fn(payload))
    else:
        return run_fn(payload)

def call_agent(module_path: str, class_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    High-level dispatcher: call local agent code or HTTP bridge depending on CREW_HTTP_BRIDGE_URL.
    """
    if CREW_HTTP_BRIDGE_URL:
        import asyncio
        agent_filename = module_path.split(".")[-1]
        return asyncio.get_event_loop().run_until_complete(_call_agent_http(agent_filename, payload))
    else:
        return _call_agent_local(module_path, class_name, payload)
    





    if _contains_forbidden(snapshot):
        raise ValueError("Snapshot contains forbidden sample data or PII keys.")


# -------------------------
# Orchestrator Agent (policy lookup moved entirely to source_schema_identifier)
# -------------------------
class OrchestratorAgent:
    """
    Orchestrator agent (synchronous) that sequences the schema drift flow.
    Policy resolution logic:
      1) input overrides (inputs["options"])
      2) job properties returned by source_schema_identifier_agent (job_node.properties)
    No local YAML defaults are used here.
    """

    def __init__(self):
        # mapping source type -> (module_path, class_name)
        self.crawler_map = {
            "db": ("schema_drift.agents.database_crawler_agent", "DatabaseCrawlerAgent"),
            "database": ("schema_drift.agents.database_crawler_agent", "DatabaseCrawlerAgent"),
            "api": ("schema_drift.agents.api_crawler_agent", "APICrawlerAgent"),
            "csv": ("schema_drift.agents.csv_crawler_agent", "CSVCrawlerAgent"),
            "file": ("schema_drift.agents.csv_crawler_agent", "CSVCrawlerAgent"),  # Entities typed 'file' treated as CSV
        }
        # other agents (module_path, class_name)
        self.source_identifier = ("schema_drift.agents.source_schema_identifier_agent", "SourceSchemaIdentifierAgent")
        self.metadata_agent = ("schema_drift.agents.metadata_agent", "MetadataAgent")
        self.detector_agent = ("schema_drift.agents.detector_agent", "DetectorAgent")
        self.healer_agent = ("schema_drift.agents.healer_agent", "HealerAgent")
        self.notification_agent = ("schema_drift.agents.notification_agent", "NotificationAgent")

    def _resolve_policies(self, input_options: Optional[Dict[str, Any]], job_props: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Precedence:
          1. explicit input_options (highest)
          2. job_props returned by source_schema_identifier_agent (metadata/catalog)
        Note: no local defaults here; callers should supply necessary policies or job_props should include defaults.
        """
        policies = {}
        # start from job_props (lowest precedence)
        if job_props:
            for k in ["auto_heal_allowed", "notify_on_breaking", "notify_channels", "operator_contact", "healing_strategy", "severity_ruleset"]:
                if k in job_props:
                    policies[k] = job_props[k]
        # override with explicit input options
        if input_options:
            for k in ["auto_heal_allowed", "notify_on_breaking", "notify_channels", "operator_contact", "healing_strategy", "severity_ruleset"]:
                if k in input_options:
                    policies[k] = input_options[k]
        # normalise boolean fields
        if "auto_heal_allowed" in policies:
            policies["auto_heal_allowed"] = bool(policies["auto_heal_allowed"])
        if "notify_on_breaking" in policies:
            policies["notify_on_breaking"] = bool(policies["notify_on_breaking"])
        # ensure notify_channels is a list if present
        if "notify_channels" in policies and not isinstance(policies["notify_channels"], list):
            policies["notify_channels"] = [policies["notify_channels"]]
        return policies

    def _call_crawler_for_source(self, source: Dict[str, Any], request_id: str) -> Optional[Dict[str, Any]]:
        """
        Given a source dictionary from source_schema_identifier, choose and invoke a crawler.
        Returns snapshot dict or None.
        """
        stype = (source.get("type") or "").lower()
        module_class = self.crawler_map.get(stype)
        if not module_class:
            logger.warning("No crawler configured for source type '%s' (source_id=%s)", stype, source.get("source_id"))
            return None

        module_path, class_name = module_class
        payload = {
            "request_id": request_id,
            "source_id": source.get("source_id"),
            "entity": source.get("entity"),
            "metadata_ref": source.get("metadata_ref"),
            "options": {"header_rows": 1, "sample_rows": 0}
        }
        logger.info("Invoking crawler %s for source %s", module_path, source.get("source_id"))
        resp = call_agent(module_path, class_name, payload)
        snapshot = resp.get("snapshot") or resp
        # validate snapshot
        assert_schema_only_snapshot(snapshot)
        return snapshot

    def run(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Synchronous run method.
        Expected inputs:
          { "pipeline": "<pipeline_name_or_id>", "request_id": "<uuid optional>", "options": {...} }
        """
        try:
            pipeline = inputs.get("pipeline")
            if not pipeline:
                raise ValueError("Missing required 'pipeline' input")
            request_id = inputs.get("request_id") or str(uuid.uuid4())
            input_options = inputs.get("options") or {}

            logger.info("Orchestration start request_id=%s pipeline=%s", request_id, pipeline)

            # 1) resolve sources via source_schema_identifier
            identifier_payload = {"request_id": request_id, "pipeline": pipeline}
            identifier_resp = call_agent(*self.source_identifier, payload=identifier_payload)
            if identifier_resp.get("error"):
                logger.error("source_schema_identifier returned error: %s", identifier_resp.get("error"))
                return {"request_id": request_id, "pipeline": pipeline, "error": identifier_resp.get("error")}

            sources = identifier_resp.get("sources", [])
            produces = identifier_resp.get("produces", [])
            transformations = identifier_resp.get("transformations", [])
            job_props = identifier_resp.get("job_node", {}).get("properties", {}) or {}

            # 2) resolve effective policies using only input_options and job_props (no local YAML)
            policies = self._resolve_policies(input_options, job_props)
            logger.debug("Resolved policies for pipeline=%s: %s", pipeline, policies)

            # 3) call crawler(s) for each source
            snapshots = []
            for s in sources:
                snap = self._call_crawler_for_source(s, request_id)
                if snap:
                    snapshots.append(snap)

            if not snapshots:
                logger.warning("No snapshots produced for pipeline=%s (request_id=%s)", pipeline, request_id)

            # 4) persist snapshots via metadata_agent
            persist_payload = {"request_id": request_id, "snapshots": snapshots}
            persist_resp = call_agent(*self.metadata_agent, payload=persist_payload)
            if persist_resp.get("error"):
                logger.error("metadata_agent returned error: %s", persist_resp.get("error"))
                return {"request_id": request_id, "pipeline": pipeline, "error": persist_resp.get("error")}
            snapshot_ids = persist_resp.get("snapshot_ids") or persist_resp.get("snapshot_id") or []

            # 5) detect drift
            detector_payload = {"request_id": request_id, "snapshot_ids": snapshot_ids, "options": {"severity_ruleset": input_options.get("severity_ruleset", "default")}}
            detector_resp = call_agent(*self.detector_agent, payload=detector_payload)
            if detector_resp.get("error"):
                logger.error("detector_agent returned error: %s", detector_resp.get("error"))
                return {"request_id": request_id, "pipeline": pipeline, "error": detector_resp.get("error")}
            drift_detected = detector_resp.get("drift_detected", False)
            drift_report = detector_resp.get("drift_report")

            healing_result = None
            notification_result = None

            # 6) healing (if allowed & drift)
            auto_heal = bool(policies.get("auto_heal_allowed", False))
            if drift_detected and auto_heal:
                healer_payload = {"request_id": request_id, "drift_report": drift_report, "impacted_lineage": produces, "options": {"auto_heal_allowed": True}}
                healing_result = call_agent(*self.healer_agent, payload=healer_payload)

            # 7) notification (if drift & notify_on_breaking)
            notify = bool(policies.get("notify_on_breaking", False))
            severity = (drift_report or {}).get("severity")
            if drift_detected and (notify or severity == "critical"):
                notify_payload = {
                    "request_id": request_id,
                    "snapshot_ids": snapshot_ids,
                    "drift_report": drift_report,
                    "severity": severity or "warning",
                    "channels": policies.get("notify_channels", ["email"]),
                    "operator": policies.get("operator_contact")
                }
                notification_result = call_agent(*self.notification_agent, payload=notify_payload)

            # 8) decision logic
            decision = "continue"
            if drift_detected:
                if auto_heal and healing_result:
                    decision = healing_result.get("next_steps") or "auto_heal"
                elif severity == "critical" or notify:
                    decision = "manual_review"
                else:
                    decision = "pause"

            result = {
                "request_id": request_id,
                "pipeline": pipeline,
                "decision": decision,
                "drift_detected": drift_detected,
                "severity": severity,
                "snapshot_ids": snapshot_ids,
                "healing": healing_result,
                "notification": notification_result
            }

            logger.info("Orchestration complete request_id=%s pipeline=%s decision=%s", request_id, pipeline, decision)
            return result

        except Exception as e:
            logger.exception("Orchestration failed: %s", str(e))
            return {"request_id": inputs.get("request_id"), "pipeline": inputs.get("pipeline"), "error": str(e)}


# -------------------------
# If you run this file directly for quick dev testing
# -------------------------
if __name__ == "__main__":
    agent = OrchestratorAgent()
    sample_input = {
        "pipeline": "CRM-To-Finance-PeopleData",
        # optional overrides to test precedence with job_props:
        # "request_id": "test-1",
        # "options": { "auto_heal_allowed": True, "notify_on_breaking": False, "notify_channels": ["email","teams"] }
    }
    out = agent.run(sample_input)
    print(json.dumps(out, indent=2))

