import uvicorn
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
from datetime import datetime
from schema_drift_detector.crew import SchemaDriftDetector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("schema_drift_detector_api")

app = FastAPI(
    title="Schema Drift Detector API",
    description="API to trigger schema drift detection pipelines using CrewAI agents.",
    version="0.1.0"
)

class DetectRequest(BaseModel):
    pipeline: str

class DetectResponse(BaseModel):
    request_id: str
    status: str
    result: Dict[str, Any]

@app.post("/detect")
async def detect_drift(request: DetectRequest):
    """
    Trigger the schema drift detection crew for a specific pipeline.
    """
    import uuid
    
    try:
        # Generate a unique request ID
        request_id = str(uuid.uuid4())
        
        logger.info("="*80)
        logger.info(f"Starting drift detection for pipeline: {request.pipeline}")
        logger.info(f"Request ID: {request_id}")
        
        inputs = {
            'pipeline': request.pipeline,
            'request_id': request_id,
            'current_year': str(datetime.now().year)
        }
        
        logger.info(f"Crew inputs: {inputs}")
        logger.info("Kicking off CrewAI workflow...")
        
        # Kickoff the crew
        result = SchemaDriftDetector().crew().kickoff(inputs=inputs)
        
        logger.info("CrewAI workflow completed")
        logger.info(f"Raw result type: {type(result)}")
        
        # CrewAI may return the result wrapped in markdown code blocks
        # Extract the actual content
        result_str = result.raw if hasattr(result, 'raw') else str(result)
        
        logger.info("="*80)
        logger.info("CREW OUTPUT:")
        logger.info(result_str)
        logger.info("="*80)
        
        # Try to parse as JSON if it's wrapped in markdown
        import re
        import json
        
        # Remove markdown code blocks if present
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', result_str, re.DOTALL)
        if json_match:
            logger.info("Found JSON in markdown code block, extracting...")
            result_data = json.loads(json_match.group(1))
        else:
            # Try to parse as JSON directly
            try:
                result_data = json.loads(result_str) if isinstance(result_str, str) else result_str
                logger.info("Parsed result as JSON directly")
            except:
                logger.warning("Could not parse result as JSON, returning raw output")
                result_data = {"raw_output": result_str}
        
        # Ensure request_id is set in the response
        if isinstance(result_data, dict):
            result_data["request_id"] = request_id
        
        logger.info(f"Final result data: {json.dumps(result_data, indent=2)}")
        logger.info(f"Drift detection completed successfully for request {request_id}")
        logger.info("="*80)
        
        return {
            "request_id": request_id,
            "status": "success",
            "result": result_data
        }

    except Exception as e:
        logger.error("="*80)
        logger.error(f"Error during drift detection for pipeline {request.pipeline}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.exception("Full traceback:")
        logger.error("="*80)
        raise HTTPException(status_code=500, detail=str(e))

def start():
    """
    Entry point for the start_api script.
    """
    uvicorn.run("schema_drift_detector.api:app", host="0.0.0.0", port=8000, reload=True)

if __name__ == "__main__":
    start()
