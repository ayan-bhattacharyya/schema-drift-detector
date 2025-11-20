import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
from datetime import datetime
from schema_drift_detector.crew import SchemaDriftDetector

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
    try:
        inputs = {
            'pipeline': request.pipeline,
            'current_year': str(datetime.now().year)
        }
        
        # Kickoff the crew
        result = SchemaDriftDetector().crew().kickoff(inputs=inputs)
        
        # CrewAI may return the result wrapped in markdown code blocks
        # Extract the actual content
        result_str = result.raw if hasattr(result, 'raw') else str(result)
        
        # Try to parse as JSON if it's wrapped in markdown
        import re
        import json
        
        # Remove markdown code blocks if present
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', result_str, re.DOTALL)
        if json_match:
            result_data = json.loads(json_match.group(1))
        else:
            # Try to parse as JSON directly
            try:
                result_data = json.loads(result_str) if isinstance(result_str, str) else result_str
            except:
                result_data = {"raw_output": result_str}
        
        return {
            "request_id": result_data.get("request_id", "unknown"),
            "status": "success",
            "result": result_data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def start():
    """
    Entry point for the start_api script.
    """
    uvicorn.run("schema_drift_detector.api:app", host="0.0.0.0", port=8000, reload=True)

if __name__ == "__main__":
    start()
