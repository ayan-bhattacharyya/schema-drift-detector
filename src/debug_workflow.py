import uuid
import json
import logging
from schema_drift_detector.agents.source_schema_identifier_agent import SourceSchemaIdentifierAgent
from schema_drift_detector.agents.csv_crawler_agent import CSVCrawlerAgent
from schema_drift_detector.agents.snapshot_persistence_agent import SnapshotPersistenceAgent
from schema_drift_detector.agents.detector_agent import DetectorAgent

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("debug_workflow")

def run_debug_workflow():
    request_id = str(uuid.uuid4())
    pipeline = "CRM-To-Finance-PeopleData"
    
    print(f"\n=== 1. Source Schema Identifier Agent (Request: {request_id}) ===")
    identifier_agent = SourceSchemaIdentifierAgent()
    identifier_output = identifier_agent.run({"pipeline": pipeline, "request_id": request_id})
    print(json.dumps(identifier_output, indent=2, default=str))
    
    integration_catalog = identifier_output.get("integration_catalog", {})
    source_type = integration_catalog.get("source_type")
    source_component = integration_catalog.get("source_component")
    
    if source_type != "CSV":
        print(f"Skipping crawler: Source type is {source_type}, expected CSV for this test.")
        return

    print(f"\n=== 2. CSV Crawler Agent ===")
    crawler_agent = CSVCrawlerAgent()
    # Construct inputs as the Orchestrator/Task would
    crawler_inputs = {
        "request_id": request_id,
        "source_id": "people-info.csv", # Derived or hardcoded for test
        "entity": "people-info.csv",
        "source_type": "csv",
        "metadata_ref": {"properties": {"source_path": source_component}},
        "options": {"header_rows": 1}
    }
    crawler_output = crawler_agent.run(crawler_inputs)
    print(json.dumps(crawler_output, indent=2, default=str))
    
    snapshot = crawler_output.get("snapshot")
    if not snapshot:
        print("Error: No snapshot returned from crawler.")
        return

    print(f"\n=== 3. Snapshot Persistence Agent ===")
    persistence_agent = SnapshotPersistenceAgent()
    persistence_inputs = {
        "request_id": request_id,
        "source_id": crawler_output.get("source_id"),
        "entity": crawler_output.get("entity"),
        "snapshot": snapshot
    }
    persistence_output = persistence_agent.persist_snapshot(persistence_inputs)
    print(json.dumps(persistence_output, indent=2, default=str))
    
    snapshot_id = persistence_output.get("snapshot_id")
    previous_snapshot_id = persistence_output.get("previous_snapshot_id")
    
    if not snapshot_id:
        print("Error: No snapshot_id returned from persistence agent.")
        return

    print(f"\n=== 4. Detector Agent ===")
    detector_agent = DetectorAgent()
    detector_inputs = {
        "request_id": request_id,
        "snapshot_id": snapshot_id,
        "previous_snapshot_id": previous_snapshot_id,
        "pipeline": pipeline
    }
    detector_output = detector_agent.run(detector_inputs)
    print(json.dumps(detector_output, indent=2, default=str))

if __name__ == "__main__":
    try:
        run_debug_workflow()
    except Exception as e:
        logger.exception("Workflow failed")
