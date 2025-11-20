import logging
import datetime
import uuid
from typing import Any, Dict, List, Optional
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine

logger = logging.getLogger("database_crawler_agent")

class DatabaseCrawlerAgent:
    """
    Crawls database sources to extract schema metadata using SQLAlchemy.
    """

    def __init__(self):
        pass

    def _get_engine(self, connection_string: str) -> Engine:
        return create_engine(connection_string)

    def run(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        request_id = inputs.get("request_id")
        source_id = inputs.get("source_id")
        entity = inputs.get("entity")
        connection_template_ref = inputs.get("connection_template_ref")
        options = inputs.get("options") or {}

        logger.info("DatabaseCrawlerAgent start request_id=%s source_id=%s entity=%s", request_id, source_id, entity)

        # In a real scenario, connection_template_ref would be resolved to a real connection string
        # For now, we assume it might be passed directly or we fail if not present
        # You might want to look up env vars based on source_id if connection_template_ref is missing
        connection_string = connection_template_ref
        
        if not connection_string:
             # Fallback: try to construct a dummy sqlite for testing if no connection string provided
             # This is just to prevent crashing if the user runs without config
             logger.warning("No connection string provided. Using in-memory SQLite for demonstration.")
             connection_string = "sqlite:///:memory:"

        try:
            engine = self._get_engine(connection_string)
            inspector = inspect(engine)
            
            # If entity is a table name, inspect it
            columns = []
            if inspector.has_table(entity):
                columns = inspector.get_columns(entity)
            else:
                logger.warning("Table %s not found in database.", entity)

            fields = []
            for i, col in enumerate(columns):
                fields.append({
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": col["nullable"],
                    "ordinal": i,
                    "hints": {
                        "default": str(col.get("default")),
                        "autoincrement": col.get("autoincrement")
                    }
                })

            snapshot = {
                "source_id": source_id,
                "entity": entity,
                "schema": {
                    "fields": fields,
                    "version_meta": {
                        "created_by": "database_crawler_agent",
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        "source_path": connection_string # Be careful not to expose secrets in real app
                    }
                }
            }

            return {
                "request_id": request_id,
                "source_id": source_id,
                "entity": entity,
                "snapshot": snapshot
            }

        except Exception as e:
            logger.error("Failed to crawl database: %s", e)
            raise