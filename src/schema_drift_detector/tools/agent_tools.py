from typing import Any, Dict, Optional, Type
from pydantic import BaseModel, Field
from crewai.tools import BaseTool

# Import existing agent classes
# Note: Adjust imports if your project structure requires different paths
from schema_drift_detector.agents.source_schema_identifier_agent import SourceSchemaIdentifierAgent
from schema_drift_detector.agents.csv_crawler_agent import CSVCrawlerAgent
from schema_drift_detector.agents.database_crawler_agent import DatabaseCrawlerAgent
from schema_drift_detector.agents.api_crawler_agent import APICrawlerAgent
from schema_drift_detector.agents.metadata_agent import MetadataAgent
from schema_drift_detector.agents.detector_agent import DetectorAgent
from schema_drift_detector.agents.healer_agent import HealerAgent
from schema_drift_detector.agents.notification_agent import NotificationAgent


# --- Source Schema Identifier Tool ---
class SourceSchemaIdentifierInput(BaseModel):
    pipeline: str = Field(..., description="The pipeline identifier or name.")
    request_id: str = Field(..., description="Unique request ID.")

class SourceSchemaIdentifierTool(BaseTool):
    name: str = "Source Schema Identifier Tool"
    description: str = (
        "Queries the GraphDB to resolve authoritative source types and entity mappings for a pipeline. "
        "Returns source details including type (db, api, csv) and metadata references."
    )
    args_schema: Type[BaseModel] = SourceSchemaIdentifierInput

    def _run(self, pipeline: str, request_id: str) -> Dict[str, Any]:
        agent = SourceSchemaIdentifierAgent()
        try:
            return agent.run({"pipeline": pipeline, "request_id": request_id})
        finally:
            if hasattr(agent, "close"):
                agent.close()


# --- CSV Crawler Tool ---
class CSVCrawlerInput(BaseModel):
    request_id: str = Field(..., description="Unique request ID.")
    source_id: str = Field(..., description="Source identifier.")
    entity: Optional[str] = Field(None, description="Entity name.")
    metadata_ref: Optional[Dict[str, Any]] = Field(default={}, description="Metadata reference containing source path properties.")
    options: Optional[Dict[str, Any]] = Field(default={}, description="Options like header_rows.")

class CSVCrawlerTool(BaseTool):
    name: str = "CSV Crawler Tool"
    description: str = (
        "Crawls CSV/flat-file sources to extract schema metadata (headers). "
        "Does NOT read data rows. Returns a canonical schema snapshot."
    )
    args_schema: Type[BaseModel] = CSVCrawlerInput

    def _run(self, request_id: str, source_id: str, entity: Optional[str] = None, metadata_ref: Optional[Dict[str, Any]] = None, options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        agent = CSVCrawlerAgent()
        inputs = {
            "request_id": request_id,
            "source_id": source_id,
            "entity": entity,
            "metadata_ref": metadata_ref or {},
            "options": options or {}
        }
        return agent.run(inputs)


# --- Database Crawler Tool ---
class DatabaseCrawlerInput(BaseModel):
    request_id: str = Field(..., description="Unique request ID.")
    source_id: str = Field(..., description="Source identifier.")
    entity: str = Field(..., description="Table or entity name.")
    connection_template_ref: Optional[str] = Field(None, description="Reference to connection template.")
    options: Optional[Dict[str, Any]] = Field(default={}, description="Options.")

class DatabaseCrawlerTool(BaseTool):
    name: str = "Database Crawler Tool"
    description: str = (
        "Crawls database sources (Postgres, MySQL, etc.) to extract schema metadata from system catalogs. "
        "Returns a canonical schema snapshot."
    )
    args_schema: Type[BaseModel] = DatabaseCrawlerInput

    def _run(self, request_id: str, source_id: str, entity: str, connection_template_ref: Optional[str] = None, options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        agent = DatabaseCrawlerAgent()
        inputs = {
            "request_id": request_id,
            "source_id": source_id,
            "entity": entity,
            "connection_template_ref": connection_template_ref,
            "options": options or {}
        }
        try:
            return agent.run(inputs)
        finally:
            if hasattr(agent, "close"):
                agent.close()


# --- API Crawler Tool ---
class APICrawlerInput(BaseModel):
    request_id: str = Field(..., description="Unique request ID.")
    source_id: str = Field(..., description="Source identifier.")
    entity: str = Field(..., description="Resource or schema name.")
    contract_ref: Optional[str] = Field(None, description="Reference to API contract.")
    options: Optional[Dict[str, Any]] = Field(default={}, description="Options.")

class APICrawlerTool(BaseTool):
    name: str = "API Crawler Tool"
    description: str = (
        "Parses API contracts (OpenAPI, etc.) to extract schema metadata. "
        "Returns a canonical schema snapshot."
    )
    args_schema: Type[BaseModel] = APICrawlerInput

    def _run(self, request_id: str, source_id: str, entity: str, contract_ref: Optional[str] = None, options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        agent = APICrawlerAgent()
        inputs = {
            "request_id": request_id,
            "source_id": source_id,
            "entity": entity,
            "contract_ref": contract_ref,
            "options": options or {}
        }
        return agent.run(inputs)


# --- Metadata Persistence Tool ---
class MetadataPersistenceInput(BaseModel):
    request_id: str = Field(..., description="Unique request ID.")
    source_id: str = Field(..., description="Source identifier.")
    entity: str = Field(..., description="Entity name.")
    snapshot: Dict[str, Any] = Field(..., description="The canonical schema snapshot to persist.")

class MetadataPersistenceTool(BaseTool):
    name: str = "Metadata Persistence Tool"
    description: str = (
        "Persists canonical schema snapshots into the GraphDB. "
        "Returns the stored snapshot ID and previous snapshot ID."
    )
    args_schema: Type[BaseModel] = MetadataPersistenceInput

    def _run(self, request_id: str, source_id: str, entity: str, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        agent = MetadataAgent()
        inputs = {
            "request_id": request_id,
            "source_id": source_id,
            "entity": entity,
            "snapshot": snapshot
        }
        try:
            return agent.persist_snapshot(inputs)
        finally:
            if hasattr(agent, "close"):
                agent.close()


# --- Drift Detection Tool ---
class DriftDetectionInput(BaseModel):
    request_id: str = Field(..., description="Unique request ID.")
    snapshot_id: str = Field(..., description="Current snapshot ID.")
    previous_snapshot_id: Optional[str] = Field(None, description="Previous snapshot ID.")
    pipeline: str = Field(..., description="Pipeline identifier for scoped detection.")
    options: Optional[Dict[str, Any]] = Field(default={}, description="Options like use_llm.")

class DriftDetectionTool(BaseTool):
    name: str = "Drift Detection Tool"
    description: str = (
        "Compares current and previous snapshots to detect schema drift. "
        "Returns a drift report."
    )
    args_schema: Type[BaseModel] = DriftDetectionInput

    def _run(self, request_id: str, snapshot_id: str, pipeline: str, previous_snapshot_id: Optional[str] = None, options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        agent = DetectorAgent()
        inputs = {
            "request_id": request_id,
            "snapshot_id": snapshot_id,
            "previous_snapshot_id": previous_snapshot_id,
            "pipeline": pipeline,
            "options": options or {}
        }
        try:
            return agent.run(inputs)
        finally:
            if hasattr(agent, "close"):
                agent.close()


# --- Healing Tool ---
class HealingInput(BaseModel):
    request_id: str = Field(..., description="Unique request ID.")
    drift_report: Dict[str, Any] = Field(..., description="The drift report.")
    impacted_lineage: Optional[list] = Field(default=[], description="List of impacted lineage items.")
    options: Optional[Dict[str, Any]] = Field(default={}, description="Options like auto_heal_allowed.")

class HealingTool(BaseTool):
    name: str = "Healing Tool"
    description: str = (
        "Generates remediation actions (SQL/dbt patches) based on the drift report. "
        "Returns recommended actions."
    )
    args_schema: Type[BaseModel] = HealingInput

    def _run(self, request_id: str, drift_report: Dict[str, Any], impacted_lineage: Optional[list] = None, options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        agent = HealerAgent()
        inputs = {
            "request_id": request_id,
            "drift_report": drift_report,
            "impacted_lineage": impacted_lineage or [],
            "options": options or {}
        }
        return agent.run(inputs)


# --- Notification Tool ---
class NotificationInput(BaseModel):
    request_id: str = Field(..., description="Unique request ID.")
    drift_report: Dict[str, Any] = Field(..., description="The drift report.")
    severity: str = Field(..., description="Severity level.")
    channels: Optional[list] = Field(default=[], description="Notification channels.")
    operator: Optional[Dict[str, Any]] = Field(default={}, description="Operator details.")

class NotificationTool(BaseTool):
    name: str = "Notification Tool"
    description: str = (
        "Sends alerts to operators via configured channels. "
        "Returns notification status."
    )
    args_schema: Type[BaseModel] = NotificationInput

    def _run(self, request_id: str, drift_report: Dict[str, Any], severity: str, channels: Optional[list] = None, operator: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        agent = NotificationAgent()
        inputs = {
            "request_id": request_id,
            "drift_report": drift_report,
            "severity": severity,
            "channels": channels or [],
            "operator": operator or {}
        }
        return agent.run(inputs)
