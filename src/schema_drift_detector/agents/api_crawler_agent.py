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
        source_type = inputs.get("source_type")
        contract_ref = inputs.get("contract_ref")
        options = inputs.get("options") or {}

        logger.info("APICrawlerAgent start request_id=%s source_id=%s entity=%s source_type=%s", request_id, source_id, entity, source_type)

        # Skip if source type is not API
        if source_type and source_type.lower() != "api":
            logger.info("Source type is '%s', not api. Skipping API crawler.", source_type)
            return {
                "request_id": request_id,
                "source_id": source_id,
                "entity": entity,
                "skipped": True,
                "reason": f"Source type is '{source_type}', not api"
            }

        # API crawler not implemented yet
        logger.warning("API crawler agent is not fully implemented yet. Skipping API crawling.")
        return {
            "request_id": request_id,
            "source_id": source_id,
            "entity": entity,
            "skipped": True,
            "not_implemented": True,
            "reason": "API crawler agent is not fully implemented yet"
        }
