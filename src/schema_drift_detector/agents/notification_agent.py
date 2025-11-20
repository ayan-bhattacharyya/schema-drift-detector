import logging
import uuid
from typing import Any, Dict, List, Optional

logger = logging.getLogger("notification_agent")

class NotificationAgent:
    """
    Sends alerts to operators via configured channels.
    
    CONDITIONAL EXECUTION:
    This agent should only send notifications if BOTH conditions are met:
    1. policies.notify_on_breaking is true (from identify_sources task)
    2. drift_detected is true (from detect_drift task)
    
    When invoked via CrewAI, the LLM will check both conditions and skip if either is false.
    """

    def __init__(self):
        pass

    def run(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        request_id = inputs.get("request_id")
        drift_report = inputs.get("drift_report") or {}
        drift_detected = inputs.get("drift_detected", False)
        notify_on_breaking = inputs.get("notify_on_breaking", False)
        severity = inputs.get("severity")
        channels = inputs.get("channels") or []
        operator = inputs.get("operator") or {}

        logger.info("NotificationAgent start request_id=%s drift_detected=%s notify_on_breaking=%s", 
                   request_id, drift_detected, notify_on_breaking)

        # Conditional: Skip if conditions not met
        if not notify_on_breaking or not drift_detected:
            reason = []
            if not notify_on_breaking:
                reason.append("notify_on_breaking=false")
            if not drift_detected:
                reason.append("no drift detected")
            
            logger.info("Skipping notification: %s", " and ".join(reason))
            return {
                "request_id": request_id,
                "skipped": True,
                "reason": f"Notification not required ({', '.join(reason)})",
                "sent": False,
                "channels": [],
                "operator_response": {
                    "decision": None,
                    "timestamp": None
                }
            }

        # Simulate sending notifications
        message = f"Schema Drift Detected! Severity: {severity}. Summary: {drift_report.get('summary')}"
        
        for channel in channels:
            logger.info("Sending notification to channel %s: %s", channel, message)

        return {
            "request_id": request_id,
            "notification_id": str(uuid.uuid4()),
            "channels": channels,
            "sent": True,
            "skipped": False,
            "operator_response": {
                "decision": None,
                "timestamp": None
            }
        }
