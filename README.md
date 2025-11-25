# Schema Drift Detector
**Version:** 0.1

---

## Table of Contents
1. [Summary](#summary)
2. [System Architecture](#system-architecture)
   - 2.1 [High-Level Architecture](#high-level-architecture)
   - 2.2 [Sequence Diagram](#sequence-diagram)
   - 2.3 [Architecture Patterns](#architecture-patterns)
3. [Technology Stack](#technology-stack)
4. [Component Design](#component-design)
5. [Development Guide](#development-guide)
   - 5.1 [Prerequisites](#prerequisites)
   - 5.2 [Installation Steps](#installation-steps)
   - 5.3 [API Endpoint Details](#api-endpoint-details)

---

## 1. Summary

The **Schema Drift Detector** is an intelligent, modular, and extensible framework designed to automatically identify, classify, and remediate schema changes ("schema drift") across diverse data sources. Built on a multi-agent architecture using **CrewAI** for orchestration, the system provides end-to-end capabilities for:

- **Schema Discovery**: Automatically crawl and extract schema metadata from databases, APIs, and CSV files
- **Change Detection**: Perform semantic comparison between schema snapshots to identify additions, deletions, and modifications
- **Impact Analysis**: Classify drift severity (breaking, moderate, non-breaking) and identify impacted downstream pipelines
- **Automated Remediation**: Generate healing scripts (SQL DDL, dbt patches) to address schema changes
- **Notification & Governance**: Alert operators about critical changes and maintain comprehensive audit trails

The system supports **database**, **API**, and **file-based** sources, uses **Neo4j** as a metadata persistence layer for historical snapshots and lineage tracking, and exposes a **FastAPI** service for integration into data pipeline workflows.

**Key Benefits:**
- Proactive detection of schema changes before they break downstream systems
- Automated generation of remediation scripts to reduce manual effort
- Historical tracking and lineage for audit and compliance
- Extensible architecture supporting multiple source types and custom detection rules

---

## 2. System Architecture

### 2.1 High-Level Architecture

*[Placeholder for High-Level Architecture Diagram]*

**Architecture Overview:**

The Schema Drift Detector follows a **multi-agent orchestration pattern** where specialized agents collaborate to perform distinct responsibilities:

1. **API Layer (FastAPI)**: Exposes REST endpoints for triggering drift detection workflows
2. **Orchestration Layer (CrewAI)**: Manages agent lifecycle and task sequencing
3. **Agent Layer**: Specialized agents for identification, crawling, persistence, detection, healing, and notification
4. **Persistence Layer (Neo4j)**: Stores schema snapshots, lineage, and metadata
5. **Source Connectors**: Pluggable adapters for databases (SQL Server, PostgreSQL, etc.), APIs (OpenAPI/Swagger), and files (CSV)

### 2.2 Sequence Diagram

*[Placeholder for Sequence Diagram]*

**Typical Workflow:**
1. API receives drift detection request with pipeline identifier
2. Source Schema Identifier Agent resolves source types and policies from Neo4j
3. Appropriate Crawler Agent(s) extract current schema metadata
4. Metadata Agent persists snapshot to Neo4j with versioning
5. Detector Agent compares current vs. previous snapshot
6. Healer Agent generates remediation scripts (if drift detected)
7. Notification Agent alerts operators (if policies require it)
8. Orchestrator Agent returns decision to caller

### 2.3 Architecture Patterns

The system implements several key architectural patterns:

#### Multi-Agent Collaboration Pattern
- **Separation of Concerns**: Each agent has a single, well-defined responsibility
- **Loose Coupling**: Agents communicate via structured JSON messages
- **Sequential Execution**: Tasks are executed in a deterministic order via CrewAI's Process.sequential

#### Metadata-Driven Configuration
- **Source Registration**: Data sources are registered in Neo4j with connection metadata
- **Policy-Based Execution**: Policies control whether auto-healing and notifications are enabled
- **Template-Based Crawling**: Connection details are resolved from templates to avoid hardcoding credentials

#### Event Sourcing for Schema History
- **Immutable Snapshots**: Each schema crawl creates a new snapshot; previous versions are never modified
- **Lineage Tracking**: Relationships between snapshots enable temporal queries and impact analysis
- **Audit Trail**: All agent actions are logged with timestamps and request IDs

#### Conditional Task Execution
- **Smart Orchestration**: Healer agent only runs if drift is detected
- **Notification Policies**: Notification agent only executes if both drift exists and policy permits
- **Source-Specific Crawling**: Only relevant crawler agents are invoked based on source type

#### LLM-Augmented Analysis
- **Semantic Detection**: Uses Google Gemini LLM to understand contextual meaning of schema changes
- **Intelligent Healing**: Generates remediation scripts with confidence scores
- **Natural Language Summaries**: Produces human-readable drift reports

---

## 3. Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Core Framework** | Python | ≥3.10, <3.15 | Primary implementation language |
| **Agent Orchestration** | CrewAI | 1.4.1 | Multi-agent workflow management and task coordination |
| **LLM Provider** | Google Gemini (via LangChain) | - | Semantic analysis, drift classification, healing script generation |
| **API Framework** | FastAPI | ≥0.100.0 | REST API for triggering drift detection workflows |
| **Web Server** | Uvicorn | ≥0.22.0 | ASGI server for FastAPI |
| **Graph Database** | Neo4j | ≥5.7.0 (Community Edition) | Schema snapshot persistence, lineage tracking, metadata catalog |
| **Data Processing** | Pandas | ≥1.5 | CSV file processing and schema inference |
| **LLM Integration** | LiteLLM | ≥1.0.0 | Unified LLM interface |
| **LLM SDK** | langchain-google-genai | ≥0.0.1 | Google Gemini integration |
| **Gemini SDK** | google-generativeai, google-genai | ≥0.3.2, ≥0.3.3 | Direct Gemini API access |
| **Configuration** | python-dotenv | ≥1.0 | Environment variable management |
| **Data Serialization** | PyYAML | ≥6.0 | Agent and task configuration files |
| **Build System** | Hatchling | - | Python package build backend |

---

## 4. Component Design

The Schema Drift Detector is composed of **nine specialized agents**, each with distinct responsibilities:

### Orchestrator Agent
**Status:** ✅ Fully Implemented

**Responsibilities:**
- Acts as the single entry point for all drift detection requests
- Receives lightweight API requests containing pipeline identifier and options
- Sequences the complete workflow: identify → crawl → persist → detect → heal → notify
- Delegates to specialized agents based on source types and policies
- Aggregates results and returns final decision (continue, pause, manual_review, auto_heal)

**Features:**
- Conditional agent invocation based on source type
- Policy-aware execution (auto_heal_allowed, notify_on_breaking)
- Comprehensive error handling and request tracking
- Returns compact JSON decision to API callers

---

### Source Schema Identifier Agent
**Status:** ✅ Fully Implemented

**Responsibilities:**
- Queries Neo4j catalog to resolve pipeline metadata
- Determines source types (database, API, CSV) for a given pipeline
- Retrieves entity mappings and connection references
- Fetches governance policies (auto-heal permissions, notification rules)
- Connect to the pipeline source code and fetch the transformation logic (⚠️ Not Implemented)

**Features:**
- Neo4j integration for metadata lookup
- Repository integration for source code lookup
- Returns structured source list with type, entity, and metadata references
- Policy resolution for downstream agents
- Supports multiple sources per pipeline

---

### CSV Crawler Agent
**Status:** ✅ Fully Implemented

**Responsibilities:**
- Inspects CSV/flat-file sources to extract schema metadata
- Reads file headers and infers column types
- Generates canonical schema model for comparison

**Features:**
- Header-based type inference
- Sample-free extraction (no PII exposure)
- Configurable header row detection
- Integration with cloud object stores (via path references)

---

### Metadata Agent
**Status:** ✅ Fully Implemented

**Responsibilities:**
- Persists canonical schema snapshots and the transformation details to Neo4j (⚠️ Persisting Transformation Details Not Implemented)
- Maintains schema version history with temporal tracking
- Manages lineage relationships between snapshots and pipelines
- Validates snapshots to ensure metadata-only content (no PII)

**Features:**
- Immutable snapshot storage
- Automatic versioning with timestamps
- Lineage graph construction
- Query support for previous snapshots and impacted pipelines

---

### Detector Agent
**Status:** ✅ Fully Implemented

**Responsibilities:**
- Compares newly stored snapshot against previous version
- Identifies additions, deletions, and modifications to fields
- Classifies severity (low, medium, high) and breaking vs. non-breaking changes
- Produces structured drift report with human-readable summaries

**Features:**
- LLM-augmented semantic analysis and field-level drift analysis
- Severity classification based on configurable rulesets
- Transformation-aware detection (identifies fields used in transformations)
- JSON output with detailed change tracking

---

### Database Crawler Agent
**Status:** ⚠️ **Not Implemented**

**Responsibilities:**
- Connect to relational database sources (PostgreSQL, MySQL, SQL Server, Cloud Database, Databricks, Snowflake, etc.)
- Extract schema metadata using system catalogs / information_schema
- Generate a canonical schema model with columns, types, constraints, and nullability

**Features:**
- ⚠️ Not Implemented

---

### API Crawler Agent
**Status:** ⚠️ **Not Implemented**

**Responsibilities:**
- Parse API contract specifications (OpenAPI/Swagger, GraphQL, RAML)
- Extract schema models, endpoints, and request/response structures
- Generate canonical contract snapshot for diffing

**Features:**
- ⚠️ Not Implemented

---

### Healer Agent
**Status:** ⚠️ **Not Implemented**

**Responsibilities:**
- Generate remediation actions based on drift reports, transformation details, and policy rules
- Produce a report that will have details, such as what has been changed, impact, and remediation DDL scripts
- Provide confidence scores and manual review flags

**Features:**
- ⚠️ Not Implemented

---

### Notification Agent
**Status:** ⚠️ **Not Implemented**

**Responsibilities:**
- Send actionable alerts to operators via configured channels
- Support multiple delivery methods, such as email, Slack, Teams, webhooks
- Capture operator responses for audit trail
- Escalate breaking changes requiring human approval

**Features:**
- ⚠️ Not Implemented

---

## 5. Development Guide

### 5.1 Prerequisites

Before setting up the Schema Drift Detector, ensure you have the following:

- **Python**: Version 3.10, 3.11, 3.12, 3.13, or 3.14 (3.11.6+ recommended)
- **Package Manager**: pip or uv (uv recommended for faster installs)
- **Neo4j Community Edition**: Version 5.7.0 or higher
  - Local installation or remote instance
  - Default bolt port: 7687
  - HTTP port: 7474
- **Google Gemini API Key**: Required for LLM-powered analysis
  - Obtain from [Google AI Studio](https://aistudio.google.com/app/apikey)
- **Virtual Environment Tool**: venv, virtualenv, or conda (optional but recommended)
- **Git**: For cloning the repository

**System Requirements:**
- 4GB+ RAM (8GB recommended for Neo4j)
- 2GB+ free disk space
- macOS, Linux, or Windows (WSL recommended for Windows)

---

### 5.2 Installation Steps

#### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd schema_drift_detector/schema_drift_detector
```

#### Step 2: Create and Activate Virtual Environment

**Using venv (Python built-in):**
```bash
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
.venv\Scripts\activate     # Windows
```

**Using conda:**
```bash
conda create -n schema-drift python=3.11
conda activate schema-drift
```

#### Step 3: Install Dependencies

**Option A: Using pip (standard)**
```bash
pip install -e .
```

**Option B: Using uv (faster)**
```bash
pip install uv
uv pip install -e .
```

**Option C: Using crewai CLI (recommended for CrewAI projects)**
```bash
pip install uv
crewai install
```

#### Step 4: Configure Neo4j

**Local Neo4j Installation:**
1. Download Neo4j Community Edition from [neo4j.com/download](https://neo4j.com/download/)
2. Start Neo4j:
   ```bash
   neo4j start
   ```
3. Open Neo4j Browser at http://localhost:7474
4. Set initial password (default username: `neo4j`)

**Using Docker (alternative):**
```bash
docker run \
  --name neo4j-schema-drift \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/yourpassword \
  -v $HOME/neo4j/data:/data \
  neo4j:5.7.0
```

#### Step 5: Configure Environment Variables

Create a `.env` file in the project root:

```bash
cp .env.example .env  # If .env.example exists
# OR create manually:
touch .env
```

Add the following configuration to `.env`:

```env
# Neo4j Configuration
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=yourpassword

# Google Gemini Configuration
GEMINI_API_KEY=your_gemini_api_key_here
GEMINI_MODEL=your_gemini_model_here

# Optional: Database Connection (for testing)
# DATABASE_CONNECTION_STRING=your_database_connection_string_here

# Optional: API Configuration
API_HOST=0.0.0.0
API_PORT=8000
```

**Important:** Never commit `.env` files to version control. Ensure `.env` is in `.gitignore`.

#### Step 6: Verify Installation

**Test Python imports:**
```bash
python -c "import schema_drift_detector; print('✓ Installation successful')"
```

**Test Neo4j connection:**
```bash
python -c "
from neo4j import GraphDatabase
uri = 'bolt://localhost:7687'
driver = GraphDatabase.driver(uri, auth=('neo4j', 'yourpassword'))
driver.verify_connectivity()
print('✓ Neo4j connection successful')
driver.close()
"
```

---

### 5.3 API Endpoint Details

The Schema Drift Detector exposes a FastAPI service for triggering drift detection workflows.

#### Starting the API Server

**Using the CLI script:**
```bash
start_api
```

**Using uvicorn directly:**
```bash
uvicorn schema_drift_detector.api:app --host 0.0.0.0 --port 8000 --reload
```

**Using the Python entry point:**
```bash
python -m schema_drift_detector.api
```

# 1. Navigate to the project
cd /Users/ayanbhattacharyya/Documents/ai-workspace/schema_drift_detector/schema_drift_detector

# 2. Activate virtual environment
source .venv/bin/activate

# 3. Go to src directory
cd src

# 4. Start the server
python -m uvicorn schema_drift_detector.api:app --host 0.0.0.0 --port 8000 --reload

# 5. Stop the server
pkill -9 uvicorn

# 1. Deactivate current venv
deactivate

# 2. Navigate to the correct directory
cd /Users/ayanbhattacharyya/Documents/ai-workspace/schema_drift_detector/schema_drift_detector

# 3. Activate the CORRECT venv
source .venv/bin/activate

# 4. Verify it's correct (should show Python 3.11 and the inner .venv path)
which python
python --version

# 5. Go to src directory
cd src

# 6. The server is ALREADY RUNNING (that's why you got "Address already in use")
# Just open that terminal to see logs, or if you want to restart:
# First stop the running one, then start fresh:
python -m uvicorn schema_drift_detector.api:app --host 0.0.0.0 --port 8000

The API will be available at:
- Base URL: http://localhost:8000
- Interactive Docs (Swagger UI): http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

---

#### Endpoints

##### `POST /detect`

Triggers the schema drift detection workflow for a specified pipeline.

**Request Body:**
```json
{
  "pipeline": "customer_data_pipeline"
}
```

**Request Schema:**
- `pipeline` (string, required): Pipeline name or identifier registered in Neo4j catalog

**Response (Success - 200 OK):**
```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "success",
  "result": {
    "request_id": "550e8400-e29b-41d4-a716-446655440000",
    "decision": "manual_review",
    "details": {
      "drift": true,
      "severity": "critical",
      "snapshot_ids": ["snapshot-abc123"],
      "healing": {
        "recommended_actions": [
          {
            "type": "sql",
            "script": "ALTER TABLE customers ADD COLUMN email VARCHAR(255);",
            "confidence": 90,
            "description": "Add missing column email"
          }
        ],
        "next_steps": "manual_review"
      }
    }
  }
}
```

**Response (Error - 500 Internal Server Error):**
```json
{
  "detail": "Error message describing what went wrong"
}
```

**Decision Values:**
- `continue`: No drift detected, pipeline can proceed
- `auto_heal`: Drift detected, healing scripts auto-applied
- `manual_review`: Drift detected, requires human review
- `pause`: Critical drift detected, pipeline should be paused

---

#### Running the Full Crew (Command Line)

**Using the CLI:**
```bash
crewai run
```

**Using the Python script:**
```bash
python -m schema_drift_detector.main
```

**Programmatic Usage:**
```python
from schema_drift_detector.crew import SchemaDriftDetector

# Create crew instance
crew = SchemaDriftDetector().crew()

# Define inputs
inputs = {
    'pipeline': 'customer_data_pipeline',
    'current_year': '2025'
}

# Execute workflow
result = crew.kickoff(inputs=inputs)
print(result)
```

---

#### Example: Testing with curl

**Detect drift for a pipeline:**
```bash
curl -X POST "http://localhost:8000/detect" \
  -H "Content-Type: application/json" \
  -d '{"pipeline": "customer_data_pipeline"}'
```

**Example with Python requests:**
```python
import requests

response = requests.post(
    "http://localhost:8000/detect",
    json={"pipeline": "customer_data_pipeline"}
)

result = response.json()
print(f"Decision: {result['result']['decision']}")
print(f"Drift Detected: {result['result']['details']['drift']}")
```

---

## Additional Resources

- **CrewAI Documentation**: https://docs.crewai.com
- **Neo4j Documentation**: https://neo4j.com/docs
- **FastAPI Documentation**: https://fastapi.tiangolo.com
- **Google Gemini API**: https://ai.google.dev

---

For questions, issues, or contributions, please open an issue in the repository or contact the development team.
