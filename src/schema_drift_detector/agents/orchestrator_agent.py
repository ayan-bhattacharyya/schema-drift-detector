from __future__ import annotations

import os
import json
import logging
import uuid
from typing import Any, Dict, Optional
from dataclasses import dataclass

# Local agent imports (assumed to be available in the same package)
# These should be the modules you already implemented: source_schema_identifier_agent, csv_crawler_agent,
# db_crawler_agent, api_crawler_agent, metadata_agent, detector_agent, healer_agent
# Import at top-level so unit tests can monkeypatch modules easily.
try:
    from schema_drift_detector.agents.source_schema_identifier_agent import SourceSchemaIdentifierAgent
    from schema_drift_detector.agents.csv_crawler_agent import CSVCrawlerAgent
    from schema_drift_detector.agents.metadata_agent import MetadataAgent
    from schema_drift_detector.agents.detector_agent import DetectorAgent
except Exception:  # pragma: no cover - if running standalone, these may not exist
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

    def run(self, pipeline: str, source_location: str, request_id: Optional[str] = None, options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Main orchestration entry.

        Args:
            pipeline: pipeline/job name (string)
            source_location: path or identifier for the source (file path, DB connection string, API URL)
            request_id: optional request id; if not provided, a new UUID is generated
            options: optional dict for passing flags through (e.g. use_llm)

        Returns: dict summarizing actions and final state
        """
        # Generate a child request id to pass to the child agents
        request_id = request_id or self._gen_request_id()
        options = options or {}
        logger.info("Orchestrator start: pipeline=%s source=%s request_id=%s", pipeline, source_location, request_id)

        # 1) Call source schema identifier agent
        if not self.source_identifier:
            raise RuntimeError("SourceSchemaIdentifierAgent not available")

        ident_input = {"pipeline": pipeline, "request_id": request_id, "source_location": source_location}
        logger.info("Calling SourceSchemaIdentifierAgent with: %s", ident_input)
        ident_out = self.source_identifier.run(ident_input)  # expected per your spec
        logger.info("Source identifier output: %s", json.dumps(ident_out, default=str))

        sources = ident_out.get("sources") or []
        if not sources:
            raise RuntimeError("SourceSchemaIdentifierAgent returned no sources for pipeline=%s" % pipeline)

        # For this orchestrator we'll only process the first source (you can extend to multiple later)
        source = sources[0]
        source_type = (source.get("type")).lower()
        entity_id = source.get("entity")
        source_id = source.get("source_id") 

        # 3) Choose appropriate crawler based on source_type
        snapshot_payload = None
        
        if source_type in ("file", "csv", "text"):  # csv/file path
            if not self.csv_crawler:
                raise RuntimeError("CsvCrawlerAgent not available")
            crawl_input = {"request_id": request_id, "source_id": source_id, "entity": entity_id, "metadata_ref": source.get("metadata_ref", {}), "options": options.get("crawler_options", {"header_rows": 1})}
            logger.info("Invoking CSV crawler: %s", crawl_input)
            crawl_out = self.csv_crawler.run(crawl_input)
            snapshot_payload = crawl_out.get("snapshot")
        elif source_type == "database" or source_type == "db":
            raise NotImplementedError("DbCrawlerAgent not implemented") 
            '''if not self.db_crawler:
                raise RuntimeError("DbCrawlerAgent not available")
            crawl_input = {"request_id": child_request_id, "source_id": source_id, "entity": source.get("entity"), "metadata_ref": source.get("metadata_ref", {}), "options": options.get("crawler_options", {})}
            logger.info("Invoking DB crawler: %s", crawl_input)
            crawl_out = self.db_crawler.run(crawl_input)
            snapshot_payload = crawl_out.get("snapshot")'''
        elif source_type in ("api", "contract", "openapi"):
            '''if not self.api_crawler:
                raise RuntimeError("ApiCrawlerAgent not available")
            crawl_input = {"request_id": child_request_id, "source_id": source_id, "entity": source.get("entity"), "metadata_ref": source.get("metadata_ref", {}), "options": options.get("crawler_options", {})}
            logger.info("Invoking API crawler: %s", crawl_input)
            crawl_out = self.api_crawler.run(crawl_input)
            snapshot_payload = crawl_out.get("snapshot")'''
            raise NotImplementedError("ApiCrawlerAgent not implemented")
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

        if not snapshot_payload:
            raise RuntimeError("Crawler did not return a snapshot for source=%s" % source_id)

        # 4) Persist snapshot via metadata agent
        if not self.metadata_store:
            raise RuntimeError("MetadataAgent not available")

        store_input = {"request_id": request_id, "source_id": source_id, "entity": snapshot_payload.get("entity"), "snapshot": snapshot_payload}
        logger.info("Storing snapshot with MetadataAgent: request_id=%s entity=%s", request_id, snapshot_payload.get("entity"))
        store_out = self.metadata_store.persist_snapshot(store_input)
        logger.info("MetadataAgent returned: %s", json.dumps(store_out, default=str))

        snapshot_id = store_out.get("snapshot_id")
        prev_snapshot_id = store_out.get("previous_snapshot_id")

        # 5) Call detector agent for drift
        if not self.detector:
            raise RuntimeError("DetectorAgent not available")

        detect_input = {"request_id": request_id, "snapshot_id": snapshot_id, "previous_snapshot_id": prev_snapshot_id, "pipeline": pipeline, "options": {"use_llm": options.get("use_llm", True)}}
        logger.info("Calling DetectorAgent: %s", detect_input)
        detect_out = self.detector.run(detect_input)
        logger.info("DetectorAgent output: %s", json.dumps(detect_out, default=str))

        drift_detected = bool(detect_out.get("drift_detected", False))

        if not drift_detected:
            logger.info("No drift detected for snapshot=%s pipeline=%s. Orchestration complete.", snapshot_id, pipeline)
            return {
                "request_id": request_id,
                "snapshot_id": snapshot_id,
                "previous_snapshot_id": prev_snapshot_id,
                "drift_detected": False,
                "detector_output": detect_out,
            }

        # 6) If drift detected, call healer agent
        if not self.healer:
            raise RuntimeError("HealerAgent not available")

        heal_input = {
            "request_id": request_id,
            "pipeline": pipeline,
            "snapshot_id": snapshot_id,
            "previous_snapshot_id": prev_snapshot_id,
            "drift_report": detect_out.get("drift_report"),
            "impacted_jobs": detect_out.get("impacted_jobs", []),
            "options": options.get("healer_options", {})
        }
        logger.info("Invoking HealerAgent with drift_report, request_id=%s", request_id)
        heal_out = self.healer.run(heal_input)
        logger.info("HealerAgent returned: %s", json.dumps(heal_out, default=str))

        return {
            "request_id": request_id,
            "snapshot_id": snapshot_id,
            "previous_snapshot_id": prev_snapshot_id,
            "drift_detected": True,
            "detector_output": detect_out,
            "healer_output": heal_out,
        }


# Quick CLI for local testing
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: orchestrator_agent.py <pipeline> <source_location>")
        sys.exit(2)

    pipeline = sys.argv[1]
    source_location = sys.argv[2]

    orchestrator = OrchestratorAgent()
    try:
        out = orchestrator.run(pipeline, source_location)
        print(json.dumps(out, indent=2, default=str))
    finally:
        # attempt graceful shutdown of child agents if they support close()
        for attr in ("source_identifier", "csv_crawler", "db_crawler", "api_crawler", "metadata_store", "detector", "healer"):
            agent = getattr(orchestrator, attr, None)
            try:
                if agent and hasattr(agent, "close"):
                    agent.close()
            except Exception:
                logger.exception("Error closing agent %s", attr)
