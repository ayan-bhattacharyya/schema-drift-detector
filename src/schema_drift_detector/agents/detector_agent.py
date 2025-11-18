from __future__ import annotations

import os
import json
import logging
import uuid
import re
import datetime
from typing import Any, Dict, List, Optional

import requests
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

try:
    from neo4j.exceptions import Neo4jError
except Exception:  # pragma: no cover - defensive fallback
    Neo4jError = Exception

logger = logging.getLogger("detector_agent")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

def get_env(key: str, default=None, required=False):
    value = os.getenv(key, default)
    if required and (value is None or str(value).strip() == ""):
        raise EnvironmentError(f"Environment variable '{key}' is required but not set.")
    return value


NEO4J_URI = get_env("NEO4J_URI", required=True)
NEO4J_USER = get_env("NEO4J_USER", required=True)
NEO4J_PASSWORD = get_env("NEO4J_PASSWORD", required=True)
NEO4J_DB = get_env("NEO4J_DB", required=True)
GEMINI_API_KEY = get_env("GEMINI_API_KEY", required=True)

# LLM / Gemini env

GEMINI_ENDPOINT = os.getenv("GEMINI_ENDPOINT")  # e.g. "https://.../predict"

def iso_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def _serialize_value(val: Any) -> Any:
    """Serialize Neo4j values to JSON-friendly primitives."""
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        try:
            return val.isoformat()
        except Exception:
            return str(val)
    if isinstance(val, (str, int, float, bool)):
        return val
    if isinstance(val, dict):
        return {k: _serialize_value(v) for k, v in val.items()}
    if isinstance(val, (list, tuple, set)):
        return [_serialize_value(v) for v in val]
    return str(val)

class DetectorAgent:
    def __init__(self) -> None:
        logger.info("DetectorAgent connecting to Neo4j at %s", NEO4J_URI)
        self._driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    def close(self) -> None:
        try:
            self._driver.close()
        except Exception:
            logger.exception("Error closing Neo4j driver")

    # --- Neo4j fetchers -------------------------------------------------
    def _tx_fetch_snapshot(self, tx, snapshot_id: str) -> Optional[Dict[str, Any]]:
        if not snapshot_id:
            return None
        q = """
        MATCH (snap:Snapshot {id:$snapshot_id})
        OPTIONAL MATCH (snap)-[:HAS_FIELD_COPY]->(sf:SnapshotField)
        RETURN snap.id AS snapshot_id, snap.source_id AS source_id, snap.entity AS entity,
               snap.timestamp AS timestamp, snap.created_by AS created_by, snap.source_path AS source_path,
               collect({
                 name: sf.name,
                 data_type: sf.data_type,
                 nullable: sf.nullable,
                 ordinal: sf.ordinal,
                 hints_json: sf.hints_json
               }) AS fields
        LIMIT 1
        """
        rec = tx.run(q, snapshot_id=snapshot_id)
        row = rec.single()
        if not row:
            return None
        out = {
            "snapshot_id": _serialize_value(row.get("snapshot_id")),
            "source_id": _serialize_value(row.get("source_id")),
            "entity": _serialize_value(row.get("entity")),
            "timestamp": _serialize_value(row.get("timestamp")),
            "created_by": _serialize_value(row.get("created_by")),
            "source_path": _serialize_value(row.get("source_path")),
            "fields": []
        }
        raw_fields = row.get("fields") or []
        for f in raw_fields:
            hints = None
            hj = f.get("hints_json")
            if hj:
                try:
                    hints = json.loads(hj)
                except Exception:
                    hints = hj
            out["fields"].append({
                "name": _serialize_value(f.get("name")),
                "type": _serialize_value(f.get("data_type")),
                "nullable": bool(f.get("nullable")) if f.get("nullable") is not None else True,
                "ordinal": int(f.get("ordinal")) if f.get("ordinal") is not None else 0,
                "hints": hints or {}
            })
        return out

    def _fetch_snapshot(self, snapshot_id: Optional[str]) -> Optional[Dict[str, Any]]:
        if not snapshot_id:
            return None
        session_kwargs = {"database": NEO4J_DB} if NEO4J_DB else {}
        with self._driver.session(**session_kwargs) as session:
            return session.execute_read(self._tx_fetch_snapshot, snapshot_id)

    def _tx_fetch_transformations_for_pipeline(self, tx, entity_name: str, pipeline: str):
        """
        Fetch only transformations that belong to the specified pipeline (job_id).
        This keeps the detection pipeline-scoped.
        """
        q = """
        MATCH (job:ETLJob {job_id:$pipeline})-[:APPLIES_TO_JOB]->(t:Transformation)
        OPTIONAL MATCH (t)-[:MAPS_SOURCE]->(sf:Field)
        OPTIONAL MATCH (t)-[:MAPS_TARGET]->(tf:Field)
        RETURN job.job_id AS job_id, t.transformation_id AS transformation_id, t.mapping_order AS mapping_order,
               t.source_field AS source_field, t.target_field AS target_field, t.expression AS expression, t.description AS transform_desc
        ORDER BY t.mapping_order
        """
        res = tx.run(q, entity_name=entity_name, pipeline=pipeline)
        out = []
        for r in res:
            out.append({k: _serialize_value(r.get(k)) for k in r.keys()})
        return out

    def _fetch_transformations_for_pipeline(self, entity_name: Optional[str], pipeline: Optional[str]) -> List[Dict[str, Any]]:
        if not entity_name or not pipeline:
            return []
        session_kwargs = {"database": NEO4J_DB} if NEO4J_DB else {}
        with self._driver.session(**session_kwargs) as session:
            return session.execute_read(self._tx_fetch_transformations_for_pipeline, entity_name, pipeline)

    def _tx_check_pipeline_uses_entity(self, tx, entity_name: str, pipeline: str) -> bool:
        q = """
        MATCH (job:ETLJob {job_id:$pipeline})-[:USES_SOURCE|:PRODUCES]->(e:Entity {name:$entity_name})
        RETURN count(job) AS cnt
        """
        r = tx.run(q, entity_name=entity_name, pipeline=pipeline)
        row = r.single()
        return bool(row and row["cnt"] and int(row["cnt"]) > 0)

    def _pipeline_uses_entity(self, entity_name: Optional[str], pipeline: Optional[str]) -> bool:
        if not entity_name or not pipeline:
            return False
        session_kwargs = {"database": NEO4J_DB} if NEO4J_DB else {}
        with self._driver.session(**session_kwargs) as session:
            return session.execute_read(self._tx_check_pipeline_uses_entity, entity_name, pipeline)

    # --- LLM call ------------------------------------------------------
    def _call_llm(self, prompt: str) -> Optional[Dict[str, Any]]:
        """
        Call LLM endpoint. Expects a JSON response body (or text that contains JSON).
        Environment: GEMINI_API_KEY and GEMINI_ENDPOINT must be set.
        The function returns parsed JSON (dict) on success, otherwise None.
        """
        if not (GEMINI_API_KEY and GEMINI_ENDPOINT):
            logger.debug("LLM not configured; skipping LLM call")
            return None

        headers = {
            "Authorization": f"Bearer {GEMINI_API_KEY}",
            "Content-Type": "application/json"
        }

        body = {
            "prompt": prompt,
            "max_tokens": 800,
            "temperature": 0.0
        }
        try:
            r = requests.post(GEMINI_ENDPOINT, headers=headers, json=body, timeout=30)
            r.raise_for_status()
            try:
                return r.json()
            except Exception:
                return {"raw_text": r.text}
        except Exception as e:
            logger.exception("LLM call failed: %s", e)
            return None

    # --- Prompt construction ------------------------------------------
    def _build_prompt(self,
                      pipeline: str,
                      before: Optional[Dict[str, Any]],
                      after: Optional[Dict[str, Any]],
                      transformations: List[Dict[str, Any]],
                      pipeline_uses_entity: bool) -> str:
        """
        Build a precise prompt that instructs the LLM to:
        - Only use metadata (no sample rows / PII)
        - Produce ONLY valid JSON with the exact schema described below
        - Apply the severity mapping table. Consider only transformations for the given pipeline.
        """
        severity_instructions = (
            "Severity mapping (pipeline-scoped):\n"
            "1) Field REMOVAL -> 'critical' if the removed field is used by any transformation for THIS pipeline; otherwise 'high'.\n"
            "2) Field TYPE CHANGE -> 'high'.\n"
            "3) Nullability change (nullable->non-nullable) -> 'high'. (non-nullable->nullable -> 'medium')\n"
            "4) Ordinal/index change only -> 'low' unless the field participates as a transformation target/source in THIS pipeline then 'medium'.\n"
            "5) Field ADDITION -> 'low' generally; if the new field conflicts with mapping targets or is required by THIS pipeline then 'medium'.\n"
            "6) Any change involving a field that participates in ANY transformation for THIS pipeline should be bumped one severity level (low->medium->high->critical), unless already 'critical'.\n\n"
        )

        dev_instructions = (
            "Return a single JSON object ONLY (no extra text) with shape:\n"
            "{\n"
            "  \"request_id\": \"<uuid>\",\n"
            "  \"drift_detected\": <bool>,\n"
            "  \"drift_report\": {\n"
            "     \"changes\": [\n"
            "       { \"op\": \"add|remove|change\", \"field\": \"<name>\", \"before\": <meta|null>, \"after\": <meta|null>, \"severity\": \"low|medium|high|critical\", \"notes\": \"...\" },\n"
            "       ...\n"
            "     ],\n"
            "     \"summary\": \"<human friendly summary>\",\n"
            "     \"severity\": \"info|warning|critical\"\n"
            "  }\n"
            "}\n\n"
            "Where 'before' and 'after' field metadata objects include at least {\"name\":\"...\",\"type\":\"...\",\"nullable\":<bool>,\"ordinal\":<int>} and MUST NOT include any sample values or PII.\n"
        )

        parts = [
            "You are a metadata-only schema drift classifier. DO NOT request or use any sample rows or PII. Work only with schema metadata.",
            f"Pipeline (evaluate impact only for this pipeline): {pipeline}",
            f"Pipeline uses entity: {pipeline_uses_entity}",
            "\n---- Before snapshot (metadata-only) ----",
            json.dumps({
                "entity": before.get("entity") if before else None,
                "source_id": before.get("source_id") if before else None,
                "fields": [
                    { "name": f.get("name"), "type": f.get("type"), "nullable": f.get("nullable"), "ordinal": f.get("ordinal") }
                    for f in (before.get("fields") if before else [])
                ]
            }, indent=2),
            "\n---- After snapshot (metadata-only) ----",
            json.dumps({
                "entity": after.get("entity") if after else None,
                "source_id": after.get("source_id") if after else None,
                "fields": [
                    { "name": f.get("name"), "type": f.get("type"), "nullable": f.get("nullable"), "ordinal": f.get("ordinal") }
                    for f in (after.get("fields") if after else [])
                ]
            }, indent=2),
            "\n---- Transformations (ONLY for this pipeline) ----",
            json.dumps(transformations, indent=2),
            "\n---- Severity rules (pipeline-scoped) ----",
            severity_instructions,
            "\n---- Output specification ----",
            dev_instructions,
            "\nReturn only JSON that strictly follows the schema above. If no drift, return drift_detected:false and an empty changes list and appropriate summary."
        ]
        return "\n\n".join(parts)

    # --- fallback deterministic diff (pipeline-scoped) -----------------
    def _fallback_diff(self, before: Optional[Dict[str, Any]], after: Optional[Dict[str, Any]], transformations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Conservative deterministic diff that only considers the provided transformations (pipeline-scoped).
        """
        before_map = { (f["name"] or "").lower(): f for f in (before.get("fields") if before else []) }
        after_map = { (f["name"] or "").lower(): f for f in (after.get("fields") if after else []) }
        changes = []

        def participates_in_pipeline(field_name: str) -> bool:
            fn = field_name or ""
            for t in transformations:
                sf = (t.get("source_field") or "") or ""
                tf = (t.get("target_field") or "") or ""
                if fn == (sf or "").lower() or fn == (tf or "").lower() or fn == (t.get("source_field") or "").lower() or fn == (t.get("target_field") or "").lower():
                    return True
            return False

        # removed and changed
        for k, bf in before_map.items():
            af = after_map.get(k)
            if not af:
                severity = "critical" if participates_in_pipeline(bf.get("name") or "") else "high"
                changes.append({"op": "remove", "field": bf.get("name"), "before": bf, "after": None, "severity": severity, "notes": "field removed (fallback, pipeline-scoped)"})
            else:
                # type change
                if (bf.get("type") or "").lower() != (af.get("type") or "").lower():
                    sev = "high"
                    if participates_in_pipeline(bf.get("name") or "") and sev != "critical":
                        # bump
                        sev = "critical" if sev == "high" else sev
                    changes.append({"op": "change", "field": bf.get("name"), "before": bf, "after": af, "severity": sev, "notes": "type changed (fallback)"})
                elif bool(bf.get("nullable")) != bool(af.get("nullable")):
                    sev = "high" if (bf.get("nullable") and not af.get("nullable")) else "medium"
                    if participates_in_pipeline(bf.get("name") or "") and sev != "critical":
                        # bump one level
                        sev = "critical" if sev == "high" else ("high" if sev == "medium" else sev)
                    changes.append({"op": "change", "field": bf.get("name"), "before": bf, "after": af, "severity": sev, "notes": "nullable changed (fallback)"})
                elif int(bf.get("ordinal") or 0) != int(af.get("ordinal") or 0):
                    sev = "medium" if participates_in_pipeline(bf.get("name") or "") else "low"
                    changes.append({"op": "change", "field": bf.get("name"), "before": bf, "after": af, "severity": sev, "notes": "ordinal changed (fallback)"})
        # additions
        for k, af in after_map.items():
            if k not in before_map:
                sev = "medium" if participates_in_pipeline(af.get("name") or "") else "low"
                changes.append({"op": "add", "field": af.get("name"), "before": None, "after": af, "severity": sev, "notes": "field added (fallback)"})

        drift_detected = len(changes) > 0
        overall = "info"
        if drift_detected:
            if any(c["op"] == "remove" for c in changes):
                overall = "critical"
            elif any(c["severity"] == "high" for c in changes):
                overall = "warning"
            else:
                overall = "info"

        summary = "No schema drift detected." if not drift_detected else " ; ".join([f"{c['op']} {c['field']} ({c['severity']})" for c in changes])
        return {
            "request_id": str(uuid.uuid4()),
            "drift_detected": drift_detected,
            "drift_report": {
                "changes": changes,
                "summary": summary,
                "severity": overall
            },
            "detected_by": "detector_agent_fallback"
        }

    # --- public run ---------------------------------------------------
    def run(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Inputs expected:
        {
          "request_id": "<uuid>",
          "snapshot_id": "<snapshot_id>",
          "previous_snapshot_id": "<previous_snapshot_id>|null",
          "pipeline": "<pipeline_name_or_id>",    # REQUIRED for pipeline-scoped evaluation
          "options": { "use_llm": True|False }
        }
        """
        request_id = inputs.get("request_id") or str(uuid.uuid4())
        snapshot_id = inputs.get("snapshot_id")
        prev_snapshot_id = inputs.get("previous_snapshot_id")
        pipeline = inputs.get("pipeline") or inputs.get("job_id")
        options = inputs.get("options") or {}
        use_llm = bool(options.get("use_llm", True))

        if not pipeline:
            raise ValueError("pipeline (job_id) is required for pipeline-scoped detection")

        logger.info("DetectorAgent.run request_id=%s snapshot=%s prev=%s pipeline=%s use_llm=%s",
                    request_id, snapshot_id, prev_snapshot_id, pipeline, use_llm)

        # fetch snapshots and metadata
        snapshot_after = self._fetch_snapshot(snapshot_id)
        snapshot_before = self._fetch_snapshot(prev_snapshot_id) if prev_snapshot_id else None

        if snapshot_after is None:
            raise ValueError(f"Snapshot not found: {snapshot_id}")

        entity = snapshot_after.get("entity") or (snapshot_before.get("entity") if snapshot_before else None)

        # Determine whether the pipeline actually uses/produces this entity. If not, still run diff
        pipeline_uses_entity = self._pipeline_uses_entity(entity, pipeline)

        # fetch only transformations that belong to this pipeline
        transformations = self._fetch_transformations_for_pipeline(entity, pipeline) if pipeline_uses_entity else []

        # For returned impacted_jobs, we keep it pipeline-scoped: return pipeline if it uses the entity, else empty list
        impacted_jobs = [pipeline] if pipeline_uses_entity else []

        # Build prompt and call LLM (if requested)
        prompt = self._build_prompt(pipeline, snapshot_before, snapshot_after, transformations, pipeline_uses_entity)
        llm_response = None
        parsed_json = None
        if use_llm:
            llm_response = self._call_llm(prompt)
            if llm_response:
                # Candidate string extraction and JSON parsing (same strategy as before)
                candidate_texts = []
                if isinstance(llm_response, dict):
                    for k in ("text", "output", "content", "body", "raw_text"):
                        if k in llm_response and isinstance(llm_response[k], str):
                            candidate_texts.append(llm_response[k])
                    if not candidate_texts:
                        try:
                            candidate_texts.append(json.dumps(llm_response))
                        except Exception:
                            pass
                else:
                    candidate_texts = [str(llm_response)]

                parsed = None
                for txt in candidate_texts:
                    try:
                        parsed = json.loads(txt)
                        parsed_json = parsed
                        break
                    except Exception:
                        m = re.search(r"\{(?:[^{}]|\{[^{}]*\})*\}", txt, flags=re.DOTALL)
                        if m:
                            js = m.group(0)
                            try:
                                parsed = json.loads(js)
                                parsed_json = parsed
                                break
                            except Exception:
                                continue
                if parsed_json is None and isinstance(llm_response, dict):
                    try:
                        choices = llm_response.get("choices")
                        if choices and isinstance(choices, list):
                            txt = None
                            c0 = choices[0]
                            if isinstance(c0, dict):
                                for key in ("message", "text", "content"):
                                    if key in c0 and isinstance(c0[key], str):
                                        txt = c0[key]
                                        break
                            if txt:
                                try:
                                    parsed_json = json.loads(txt)
                                except Exception:
                                    m = re.search(r"\{(?:[^{}]|\{[^{}]*\})*\}", txt, flags=re.DOTALL)
                                    if m:
                                        try:
                                            parsed_json = json.loads(m.group(0))
                                        except Exception:
                                            parsed_json = None
                    except Exception:
                        parsed_json = None

        # If valid parsed_json, validate and shape it
        if parsed_json and isinstance(parsed_json, dict):
            drift_report = parsed_json.get("drift_report") or parsed_json.get("report") or parsed_json
            try:
                out = {
                    "request_id": parsed_json.get("request_id") or request_id,
                    "drift_detected": bool(parsed_json.get("drift_detected") or (drift_report and len(drift_report.get("changes", [])) > 0)),
                    "drift_report": {
                        "changes": drift_report.get("changes", []) if drift_report else parsed_json.get("changes", []),
                        "summary": (drift_report.get("summary") if drift_report else parsed_json.get("summary")) or "",
                        "severity": (drift_report.get("severity") if drift_report else parsed_json.get("severity")) or "info"
                    },
                    "impacted_jobs": impacted_jobs,
                    "detected_by": "detector_agent_llm"
                }
                normalized_changes = []
                for c in out["drift_report"]["changes"]:
                    normalized_changes.append({
                        "op": c.get("op"),
                        "field": c.get("field"),
                        "before": c.get("before"),
                        "after": c.get("after"),
                        "severity": c.get("severity"),
                        "notes": c.get("notes") or c.get("reason") or ""
                    })
                out["drift_report"]["changes"] = normalized_changes
                logger.info("DetectorAgent: LLM produced %d changes (request_id=%s)", len(normalized_changes), request_id)
                return out
            except Exception:
                logger.exception("Parsed LLM JSON invalid shape; falling back to deterministic diff")

        # fallback
        fallback = self._fallback_diff(snapshot_before, snapshot_after, transformations)
        fallback["request_id"] = request_id
        fallback["impacted_jobs"] = impacted_jobs
        fallback["detected_by"] = "detector_agent_fallback"
        logger.info("DetectorAgent: returning fallback diff (request_id=%s)", request_id)
        return fallback


# quick CLI dev invocation
if __name__ == "__main__":
    import sys

    agent = DetectorAgent()
    try:
        example = {
            "request_id": "local-detect-1",
            "snapshot_id": "snapshot_peopleinfo_v1",   # replace with actual snapshot ids from metadata_agent
            "previous_snapshot_id": None,
            "pipeline": "CRM-To-Finance-PeopleData",
            "options": {"use_llm": True}
        }
        res = agent.run(example)
        print(json.dumps(res, indent=2))
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        raise
    finally:
        agent.close()
