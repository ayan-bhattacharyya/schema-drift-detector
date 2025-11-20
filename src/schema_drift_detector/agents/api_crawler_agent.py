import logging
import datetime
import requests
import yaml
import json
from typing import Any, Dict, List, Optional

logger = logging.getLogger("api_crawler_agent")

class APICrawlerAgent:
    """
    Crawls API sources (OpenAPI/Swagger) to extract schema metadata.
    """

    def __init__(self):
        pass

    def _fetch_spec(self, contract_ref: str) -> Dict[str, Any]:
        """
        Fetch and parse the API specification.
        contract_ref can be a URL or a local file path.
        """
        content = ""
        if contract_ref.startswith("http://") or contract_ref.startswith("https://"):
            response = requests.get(contract_ref)
            response.raise_for_status()
            content = response.text
        else:
            with open(contract_ref, 'r') as f:
                content = f.read()
        
        try:
            return yaml.safe_load(content)
        except yaml.YAMLError:
            return json.loads(content)

    def run(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        request_id = inputs.get("request_id")
        source_id = inputs.get("source_id")
        entity = inputs.get("entity")
        contract_ref = inputs.get("contract_ref")
        options = inputs.get("options") or {}

        logger.info("APICrawlerAgent start request_id=%s source_id=%s entity=%s", request_id, source_id, entity)

        if not contract_ref:
            logger.warning("No contract_ref provided. Returning empty snapshot.")
            return {
                "request_id": request_id,
                "source_id": source_id,
                "entity": entity,
                "snapshot": {}
            }

        try:
            spec = self._fetch_spec(contract_ref)
            
            # Basic extraction: get models/schemas
            models = {}
            components = spec.get("components", {})
            schemas = components.get("schemas", {})
            
            # Also check definitions (Swagger 2.0)
            if not schemas:
                schemas = spec.get("definitions", {})

            for name, schema in schemas.items():
                fields = []
                properties = schema.get("properties", {})
                for prop_name, prop_details in properties.items():
                    fields.append({
                        "name": prop_name,
                        "type": prop_details.get("type", "unknown"),
                        "nullable": not prop_details.get("required", False) # Simplified assumption
                    })
                models[name] = {"fields": fields}

            # If entity matches a model name, return that model's fields as the main fields
            # Otherwise, return all models in the snapshot
            main_fields = []
            if entity in models:
                main_fields = models[entity]["fields"]
            
            snapshot = {
                "source_id": source_id,
                "entity": entity,
                "schema": {
                    "fields": main_fields, # For compatibility with other agents expecting flat fields
                    "models": models,
                    "version_meta": {
                        "created_by": "api_crawler_agent",
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        "source_path": contract_ref
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
            logger.error("Failed to crawl API: %s", e)
            raise
