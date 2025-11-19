from __future__ import annotations

import os
import json
import logging
import uuid
import re
import datetime

# Only used if GEMINI_API_KEY and GEMINI_MODEL are present
try:
    import google.generativeai as genai  # type: ignore
    _GENAI_AVAILABLE = True
except Exception:
    genai = None  # type: ignore
    _GENAI_AVAILABLE = False
import requests

from typing import Any, Dict, List, Optional
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
GEMINI_MODEL = get_env("GEMINI_MODEL", required=True)

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

        # Configure Gemini SDK once (if available and configured)
        if _GENAI_AVAILABLE and GEMINI_API_KEY and GEMINI_MODEL:
            try:
                genai.configure(api_key=GEMINI_API_KEY)
                # instantiate model handle lazily later — don't error here if model name is bad
                self._gemini_model_name = GEMINI_MODEL
                logger.debug("Gemini SDK configured for model=%s", GEMINI_MODEL)
            except Exception as e:
                logger.warning("Gemini SDK configuration failed: %s", e)
                self._gemini_model_name = None
        else:
            self._gemini_model_name = None
            if not _GENAI_AVAILABLE:
                logger.debug("Gemini SDK not installed; LLM calls will be skipped.")
            elif not GEMINI_API_KEY or not GEMINI_MODEL:
                logger.debug("Gemini SDK not configured via env vars; LLM calls will be skipped.")

    def close(self) -> None:
        try:
            self._driver.close()
        except Exception:
            logger.exception("Error closing Neo4j driver")

    # --- Neo4j fetchers -------------------------------------------------
    def _tx_fetch_snapshot(self, tx, snapshot_id: str) -> Optional[Dict[str, Any]]:
        """
        CHANGED: Robust fetch of Snapshot and its SnapshotField copies.
        - Use consistent alias 'fields' in the Cypher so python looks it up correctly.
        - Tolerant to properties like 'source' vs 'source_id' and SnapshotField shapes.
        - Logs returned row keys for debugging.
        """
        if not snapshot_id:
            return None

        q = """
        MATCH (snap:Snapshot {id:$snapshot_id})
        OPTIONAL MATCH (snap)-[:HAS_FIELD_COPY]->(sf:SnapshotField)
        RETURN
           snap.id AS snapshot_id,
           coalesce(snap.source_id, snap.source) AS source_id,
           coalesce(snap.entity, snap.source) AS entity,
           snap.timestamp AS timestamp,
           snap.created_by AS created_by,
           snap.source_path AS source_path,
           collect(properties(sf)) AS fields
        LIMIT 1
        """
        rec = tx.run(q, snapshot_id=snapshot_id)
        row = rec.single()

        # DEBUG: log returned column keys to help debugging in future runs
        try:
            row_keys = list(row.keys()) if row is not None else []
        except Exception:
            row_keys = []
        logger.debug("Cypher row keys for snapshot_id=%s : %s", snapshot_id, row_keys)

        if not row:
            logger.debug("No snapshot record returned for id=%s", snapshot_id)
            return None

        out = {
            "snapshot_id": _serialize_value(row.get("snapshot_id")),
            # tolerate 'source_id' or earlier 'source' property via coalesce in query
            "source_id": _serialize_value(row.get("source_id")),
            # tolerate 'entity' or fallback to 'source'
            "entity": _serialize_value(row.get("entity")),
            "timestamp": _serialize_value(row.get("timestamp")),
            "created_by": _serialize_value(row.get("created_by")),
            "source_path": _serialize_value(row.get("source_path")),
            "fields": []
        }

        raw_fields = row.get("fields") or []
        # raw_fields is expected to be a list of property maps, but be tolerant of other shapes
        if not raw_fields:
            raw_fields = []

        for f in raw_fields:
            if not f:
                continue
            # f should already be a dict because we used properties(sf) in Cypher,
            # but remain defensive: if it's a Node-like object, try to get properties
            if isinstance(f, dict):
                props = f
            else:
                try:
                    # Node-like support (neo4j internal)
                    props = dict(f)
                except Exception:
                    try:
                        props = dict(getattr(f, "_properties", {}))
                    except Exception:
                        props = {}

            # tolerant hints extraction
            hj = props.get("hints_json") if "hints_json" in props else props.get("hints")
            hints = {}
            if hj:
                if isinstance(hj, str):
                    try:
                        hints = json.loads(hj)
                    except Exception:
                        # keep raw string under 'raw' to avoid data loss
                        hints = {"raw": hj}
                elif isinstance(hj, dict):
                    hints = hj
                else:
                    hints = {"value": str(hj)}

            out["fields"].append({
                "name": _serialize_value(props.get("name") or props.get("field_name") or props.get("id") or ""),
                "type": _serialize_value(props.get("data_type") or props.get("dataType") or props.get("type") or ""),
                "nullable": bool(props.get("nullable")) if props.get("nullable") is not None else True,
                "ordinal": int(props.get("ordinal")) if props.get("ordinal") is not None else 0,
                "hints": hints or {}
            })

        logger.debug("Fetched snapshot %s: fields_count=%d", snapshot_id, len(out["fields"]))
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
        Use Google Gemini Python SDK if available and configured.
        - Returns dict parsed from LLM JSON when possible, otherwise returns {"raw_text": "..."}.
        - Falls back to the old REST path only if GEMINI_ENDPOINT is not set and SDK not available.
        """
        # Prefer SDK if configured
        if self._gemini_model_name and _GENAI_AVAILABLE:
            try:
                # Use SDK model handle and generate content
                genai.configure(api_key=GEMINI_API_KEY)  # ensure configured
                generation_config = genai.GenerationConfig(max_output_tokens=1024,temperature=0.0)
                model = genai.GenerativeModel(model_name=self._gemini_model_name,generation_config=generation_config)
                # call LLM via SDK (temperature 0 to be deterministic)
                
                sdk_resp = model.generate_content(prompt)
                logger.info("Gemini SDK response: %s", sdk_resp)

                candidate_texts: List[str] = []
                # 1) If SDK returned a high-level .text attribute
                try:
                    if hasattr(sdk_resp, "text") and isinstance(getattr(sdk_resp, "text"), str):
                        candidate_texts.append(sdk_resp.text)
                except Exception:
                    pass

                # 2) If SDK returned .result.candidates -> content.parts[*].text (proto)
                try:
                    # Many SDK responses are proto-like: sdk_resp.result.candidates -> list of candidate protos
                    res = getattr(sdk_resp, "result", None)
                    if res is not None:
                        cand_list = getattr(res, "candidates", None) or []
                        for cand in cand_list:
                            # candidate.content.parts is common
                            content = getattr(cand, "content", None)
                            if content:
                                parts = getattr(content, "parts", None) or []
                                for p in parts:
                                    text_part = getattr(p, "text", None)
                                    if isinstance(text_part, str) and text_part.strip():
                                        candidate_texts.append(text_part)
                except Exception:
                    # defensive: ignore proto introspection errors
                    logger.debug("Error while extracting candidates from SDK proto", exc_info=True)

                # 3) If SDK is dict-like or has __dict__, try its dict conversion
                try:
                    if not candidate_texts and hasattr(sdk_resp, "__dict__"):
                        resp_map = dict(sdk_resp.__dict__)
                        # look for common keys
                        for key in ("text", "output", "content", "body", "raw_text", "candidates"):
                            v = resp_map.get(key)
                            if isinstance(v, str) and v.strip():
                                candidate_texts.append(v)
                            elif isinstance(v, (list, tuple)) and v:
                                # try flattening a first candidate
                                first = v[0]
                                if isinstance(first, dict):
                                    for kk in ("text", "content"):
                                        if kk in first and isinstance(first[kk], str) and first[kk].strip():
                                            candidate_texts.append(first[kk])
                                            break
                except Exception:
                    pass
                # 4) last-resort: stringifying the sdk_resp
                if not candidate_texts:
                    try:
                        txt = str(sdk_resp)
                        if txt and txt.strip():
                            candidate_texts.append(txt)
                    except Exception:
                        pass
                
                cleaned_texts = []
                for t in candidate_texts:
                    if not isinstance(t, str):
                        continue
                    s = t.strip()

                    # Remove triple-backtick code fences, with optional "json" after opening fence
                    m = re.search(r"```(?:json)?\n(.*)```$", s, flags=re.DOTALL | re.IGNORECASE)
                    if m:
                        s = m.group(1).strip()
                    else:
                        # also remove single-line fences or inline ```json ... ```
                        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE)
                        s = re.sub(r"\s*```$", "", s, flags=re.IGNORECASE)

                    # If the model enclosed in markdown code block with indentation, strip common fences
                    s = s.strip()
                    cleaned_texts.append(s)

                # DEBUG: show cleaned candidate texts
                logger.debug("Cleaned candidate texts count=%d", len(cleaned_texts))
                for i, ct in enumerate(cleaned_texts[:3]):
                    logger.debug("Cleaned candidate[%d] preview: %.400s", i, ct)

                # CHANGED: attempt JSON parse on each cleaned text
                for ct in cleaned_texts:
                    if not ct:
                        continue
                    # direct parse
                    try:
                        parsed = json.loads(ct)
                        logger.debug("Parsed JSON from Gemini candidate (direct)")
                        return parsed
                    except Exception:
                        # try to extract first JSON object using regex (more tolerant)
                        m = re.search(r"(\{(?:[^{}]|\{[^{}]*\})*\})", ct, flags=re.DOTALL)
                        if m:
                            js = m.group(1)
                            try:
                                parsed = json.loads(js)
                                logger.debug("Parsed JSON from Gemini candidate (regex-extracted)")
                                return parsed
                            except Exception:
                                # continue to next candidate
                                logger.debug("Regex-extracted JSON failed to json.loads(); continuing", exc_info=True)
                                continue

                # If nothing parsed, return the first cleaned text for inspection
                if cleaned_texts:
                    return {"raw_text": cleaned_texts[0]}

                # No content available
                logger.debug("Gemini SDK returned no usable textual candidates")
                return None

            except Exception as e:
                logger.exception("Gemini SDK call failed: %s", e)
                return None

    # --- Prompt construction ------------------------------------------
    def _build_prompt(self,
                    pipeline: str,
                    before: Optional[Dict[str, Any]],
                    after: Optional[Dict[str, Any]],
                    transformations: List[Dict[str, Any]],
                    pipeline_uses_entity: bool) -> str:
        """
        Improved LLM prompt with few-shot examples.
        Replace the existing _build_prompt with this function.
        This function uses deterministic instructions + 4 few-shot JSON examples
        that strongly bias the model toward returning strictly-formed JSON.

        Inputs:
        - pipeline: pipeline/job id (string)
        - before: snapshot BEFORE (metadata-only dict or None)
        - after: snapshot AFTER (metadata-only dict or None)
        - transformations: list of transformation mappings for THIS pipeline
        - pipeline_uses_entity: bool
        """
        # helper to pretty-print provided JSON blocks
        before_json = json.dumps({
            "entity": before.get("entity") if before else None,
            "source_id": before.get("source_id") if before else None,
            "fields": [
                {"name": f.get("name"), "type": f.get("type"), "nullable": f.get("nullable"), "ordinal": f.get("ordinal")}
                for f in (before.get("fields") if before else [])
            ]
        }, indent=2)

        after_json = json.dumps({
            "entity": after.get("entity") if after else None,
            "source_id": after.get("source_id") if after else None,
            "fields": [
                {"name": f.get("name"), "type": f.get("type"), "nullable": f.get("nullable"), "ordinal": f.get("ordinal")}
                for f in (after.get("fields") if after else [])
            ]
        }, indent=2)

        trans_json = json.dumps(transformations or [], indent=2)

        # Few-shot examples (4 concise examples demonstrating add/remove/change + transformation bump)
        few_shot = r"""
    EXAMPLE 1 — Addition (must be reported)
    Before:
    {
    "entity":"people-info.csv",
    "fields": [
        {"name":"name","type":"string","nullable":false,"ordinal":0}
    ]
    }
    After:
    {
    "entity":"people-info.csv",
    "fields": [
        {"name":"name","type":"string","nullable":false,"ordinal":0},
        {"name":"country","type":"string","nullable":true,"ordinal":1}
    ]
    }
    Transformations: []
    Expected output:
    {
    "request_id": "EX-ADD-1",
    "drift_detected": true,
    "drift_report": {
        "changes": [
        {
            "op": "add",
            "field": "country",
            "before": null,
            "after": {"name":"country","type":"string","nullable":true,"ordinal":1},
            "severity": "low",
            "notes": "field added"
        }
        ],
        "summary": "add country (low)",
        "severity": "info"
    }
    }

    EXAMPLE 2 — Removal used in a transformation (critical)
    Before:
    {
    "entity":"people-info.csv",
    "fields": [
        {"name":"name","type":"string","nullable":false,"ordinal":0},
        {"name":"date_of_birth","type":"date","nullable":false,"ordinal":1}
    ]
    }
    After:
    {
    "entity":"people-info.csv",
    "fields": [
        {"name":"date_of_birth","type":"date","nullable":false,"ordinal":1}
    ]
    }
    Transformations: [
    {"source_field": "name", "target_field": "firstname", "expression":"..."}
    ]
    Expected output:
    {
    "request_id": "EX-REMOVE-1",
    "drift_detected": true,
    "drift_report": {
        "changes": [
        {
            "op": "remove",
            "field": "name",
            "before": {"name":"name","type":"string","nullable":false,"ordinal":0},
            "after": null,
            "severity": "critical",
            "notes": "field removed and used by transformations for this pipeline"
        }
        ],
        "summary": "remove name (critical)",
        "severity": "critical"
    }
    }

    EXAMPLE 3 — Type change (high)
    Before:
    {
    "entity":"people-info.csv",
    "fields":[{"name":"age","type":"int","nullable":true,"ordinal":2}]
    }
    After:
    {
    "entity":"people-info.csv",
    "fields":[{"name":"age","type":"string","nullable":true,"ordinal":2}]
    }
    Transformations: []
    Expected output:
    {
    "request_id": "EX-TYPE-1",
    "drift_detected": true,
    "drift_report": {
        "changes": [
        {
            "op": "change",
            "field": "age",
            "before": {"name":"age","type":"int","nullable":true,"ordinal":2},
            "after": {"name":"age","type":"string","nullable":true,"ordinal":2},
            "severity": "high",
            "notes": "type changed int -> string"
        }
        ],
        "summary": "change age (high)",
        "severity": "warning"
    }
    }

    EXAMPLE 4 — Nullable + ordinal change, participates in transform (bumped -> critical)
    Before:
    {
    "entity":"people-info.csv",
    "fields":[{"name":"company","type":"string","nullable":true,"ordinal":3}]
    }
    After:
    {
    "entity":"people-info.csv",
    "fields":[{"name":"company","type":"string","nullable":false,"ordinal":4}]
    }
    Transformations: [
    {"source_field":"company","target_field":"company","expression":"..."}
    ]
    Expected output:
    {
    "request_id": "EX-CHANGE-1",
    "drift_detected": true,
    "drift_report": {
        "changes": [
        {
            "op": "change",
            "field": "company",
            "before": {"name":"company","type":"string","nullable":true,"ordinal":3},
            "after": {"name":"company","type":"string","nullable":false,"ordinal":4},
            "severity": "critical",
            "notes": "nullable changed (nullable->non-nullable) and ordinal changed; field participates in pipeline transformations -> bump severity"
        }
        ],
        "summary": "change company (critical)",
        "severity": "critical"
    }
    }
    """

        # Core instructions (hard rules and output schema)
        instructions = (
            "You are a metadata-only schema drift classifier. DO NOT request or use any sample rows or PII. Work only with schema metadata.\n\n"
            "Hard rules you MUST follow:\n"
            "1) ALWAYS return exactly one JSON object and nothing else (no surrounding text, no explanation).\n"
            "2) The JSON object MUST follow this exact shape (keys/types):\n"
            "{\n"
            "  \"request_id\": \"<uuid>\",\n"
            "  \"drift_detected\": <bool>,\n"
            "  \"drift_report\": {\n"
            "    \"changes\": [ { \"op\": \"add|remove|change\", \"field\": \"<name>\", \"before\": <meta|null>, \"after\": <meta|null>, \"severity\": \"low|medium|high|critical\", \"notes\": \"<short reason>\" }, ... ],\n"
            "    \"summary\": \"<single-sentence human summary>\",\n"
            "    \"severity\": \"info|warning|critical\"\n"
            "  }\n"
            "}\n\n"
            "3) If any structural difference exists between the before and after snapshots' fields arrays, you MUST set drift_detected:true and include a change entry for each difference (add/remove/change).\n"
            "4) Compare field names case-insensitively. Treat lists as sets for add/remove detection (ignore ordering for add/remove), but still report ordinal changes as a 'change'.\n"
            "5) Include at least {\"name\",\"type\",\"nullable\",\"ordinal\"} in before/after field meta; DO NOT include sample data.\n\n"
            "Severity mapping (pipeline-scoped):\n"
            "- Field REMOVAL: 'critical' if removed field participates in any transformation for THIS pipeline; otherwise 'high'.\n"
            "- Field TYPE CHANGE -> 'high'.\n"
            "- Nullability: nullable->non-nullable => 'high'; non-nullable->nullable => 'medium'.\n"
            "- Ordinal only -> 'low', unless field participates in a transformation for THIS pipeline then 'medium'.\n"
            "- Field ADDITION -> 'low' generally; if required by or conflicting with THIS pipeline mapping -> 'medium'.\n\n"
            "Transformation bump rule: After computing base severity, bump one level (low->medium->high->critical) if the field participates in ANY transformation for THIS pipeline (source or target), unless already 'critical'.\n\n"
            "Overall drift_report.severity: 'critical' if any change is 'critical' or any removal is 'critical'; else 'warning' if any change is 'high'; else 'info'.\n\n"
            "Now follow the examples above and then analyze the provided CASE (below). Return EXACTLY ONE JSON object following the schema.\n\n"
        )

        # Compose final prompt: examples first, then instructions, then the CASE (the real before/after/transformations)
        parts = [
            few_shot,
            instructions,
            "---- CONTEXT (evaluate impact ONLY for this pipeline) ----",
            f"Pipeline: {pipeline}",
            f"Pipeline uses entity: {bool(pipeline_uses_entity)}",
            "\n---- BEFORE ----",
            before_json,
            "\n---- AFTER ----",
            after_json,
            "\n---- TRANSFORMATIONS (ONLY for this pipeline) ----",
            trans_json,
            "\n---- END CONTEXT ----",
            "\nReturn only the single JSON object (no commentary)."
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
            logger.info("DetectorAgent: calling LLM for drift detection with prompt: %s", prompt)
            # call LLM
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
            "snapshot_id": "78eabf0e-237a-4dc2-96db-2c7430074f17",   # replace with actual snapshot ids from metadata_agent
            "previous_snapshot_id": "snapshot_peopleinfo_v1", # replace with actual previous snapshot id
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
