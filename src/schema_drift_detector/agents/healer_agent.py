import logging
import uuid
from typing import Any, Dict, List, Optional

logger = logging.getLogger("healer_agent")

class HealerAgent:
    """
    Generates remediation actions (SQL/dbt patches) based on the drift report.
    
    CONDITIONAL EXECUTION:
    This agent should only generate healing actions if drift_detected is true.
    When invoked via CrewAI, the LLM will check the detect_drift task output
    and skip execution if no drift was detected.
    """

    def __init__(self):
        pass

    def run(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        request_id = inputs.get("request_id")
        drift_report = inputs.get("drift_report") or {}
        drift_detected = inputs.get("drift_detected", False)
        impacted_lineage = inputs.get("impacted_lineage") or []
        options = inputs.get("options") or {}

        logger.info("HealerAgent start request_id=%s drift_detected=%s", request_id, drift_detected)

        # Conditional: Skip if no drift detected
        if not drift_detected:
            logger.info("No drift detected, skipping healing generation")
            return {
                "request_id": request_id,
                "healing": {
                    "recommended_actions": [],
                    "next_steps": "none"
                },
                "skipped": True,
                "reason": "No drift detected",
                "generated_by": "healer_agent"
            }

        changes = drift_report.get("changes", [])
        actions = []

        for change in changes:
            op = change.get("op")
            field = change.get("field")
            after = change.get("after") or {}
            field_type = after.get("type")

            if op == "add":
                # Generate basic SQL ADD COLUMN
                script = f"ALTER TABLE {{table_name}} ADD COLUMN {field} {field_type};"
                actions.append({
                    "type": "sql",
                    "script": script,
                    "confidence": 90,
                    "description": f"Add missing column {field}"
                })
            elif op == "change":
                # Generate basic SQL ALTER COLUMN TYPE
                script = f"ALTER TABLE {{table_name}} ALTER COLUMN {field} TYPE {field_type};"
                actions.append({
                    "type": "sql",
                    "script": script,
                    "confidence": 70,
                    "description": f"Change type of column {field} to {field_type}"
                })
            elif op == "remove":
                 actions.append({
                    "type": "manual",
                    "script": f"-- Manual review required for removed column: {field}",
                    "confidence": 100,
                    "description": f"Column {field} was removed. Review required."
                })

        return {
            "request_id": request_id,
            "healing": {
                "recommended_actions": actions,
                "next_steps": "manual_review" if any(a["type"] == "manual" for a in actions) else "auto_heal"
            },
            "skipped": False,
            "generated_by": "healer_agent"
        }
