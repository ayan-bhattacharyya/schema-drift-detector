# Schema Drift Detector Agent

## Overview

The **Schema Drift Detector Agent** is an intelligent, modular, and extensible framework designed to automatically identify, classify, and remediate schema changes ("schema drift") across diverse data sources. It leverages **CrewAI** for agent orchestration, **Neo4j** for metadata persistence, and a suite of purpose-built agents to detect drifts, classify severity, and optionally generate healing scripts.

The design supports database, API, and file-based crawlers, a source-driven schema identifier agent, a persistence layer for historical snapshots, a detector agent for semantic difference classification on schema or transformation level,  a healer agent to generate corrective scripts, and a notification agent to notify the operator regarding the change.

---

## Installation

### Prerequisites

* Python 3.10 or above
* Pip or Poetry
* Neo4j Community Edition (local or remote)
* Virtual environment tool (optional)

### Steps

1. **Clone the Repository**

   ```bash
   git clone <repo-url>
   cd schema-drift-detector
   ```

2. **Create a Virtual Environment (optional)**

   ```bash
   python -m venv venv
   source venv/bin/activate  # macOS/Linux
   venv\Scripts\activate     # Windows
   ```

3. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Neo4j**

   * Start Neo4j Community Edition.
   * Set your username/password.
   * Update your project `.env` file:

   ```env
   NEO4J_URI=bolt://localhost:7687
   NEO4J_USER=neo4j
   NEO4J_PASSWORD=yourpassword
   ```

5. **Run the Agent System**

   ```bash
   python main.py
   ```

---

## Components Used

* **CrewAI**
  Agent orchestration and workflow management.

* **Neo4j Community Database**
  Stores schema snapshots, relationships, metadata lineage, and detection logs.

* **Python**
  Core implementation language for all agents and orchestration logic.

* **Other Libraries**

  * Pandas (CSV crawler)
  * Pydantic (schema models)
  * FastAPI (optional for exposing API)
  * LangChain / LLM wrappers (optional)

---

## Agents & Responsibilities

### 1. Source Schema Identifier Agent

* Identifies source system type (CSV, API, database).
* Extracts schema information.
* Generates metadata models (entities, fields, types, transformations).

### 2. Orchestrator Agent

* Manages the lifecycle of the drift detection pipeline.
* Invokes identifier, crawler, persistence, detector, healer, and notifier agents.

### 3. Crawler Agents

* **CSV Crawler (Implemented)**: Reads headers, infers types, extracts sample data, builds schema.
* **Database Crawler (Not Implemented)**: Would connect to RDBMS sources and infer schema.
* **API Crawler (Not Implemented)**: Would fetch schema via OpenAPI/metadata endpoints.

### 4. Metadata Snapshot Persistence Agent

* Stores extracted schemas from any crawler into Neo4j.
* Maintains historical lineage for each pipeline.
* Supports diff comparison between any two snapshots.

### 5. Detector Agent

* Performs semantic diff between snapshots.
* Detects new/removed/modified fields.
* Calculates severity levels (e.g., breaking, moderate, non-breaking).
* Understands transformation-associated fields (prototype partial).

### 6. Healer Agent

* Generates healing scripts (prototype supports CSV-driven transformations).
* Includes add/remove/modify-field operations.
* Designed to support SQL or API-based healing for future extensions.

### 7. Notification Agent (Not Implemented)

* Intended to send alerts to notification channels, such as Slack, email, Teams, etc.

---

## Prototype Implementation (Current State)

The following features have been **fully implemented in the prototype**:

### ✔ Extract Schema & Field Metadata

Using the **Source Schema Identifier Agent**, metadata such as entities, fields, types, and transformations is extracted.

### ✔ Crawl CSV files

The **CSV Crawler Agent** reads file-based sources, extracts schema, and prepares metadata for persistence.

### ✔ Persist Snapshot in Neo4j

The **Persistence Agent** stores entity, field, relationship, and transformation metadata as graph structures.

### ✔ Detect Schema Differences

The **Detector Agent** compares two snapshots and:

* Identifies additions, deletions, and modifications.
* Performs semantic detection (e.g., ordinal changes, transformation-impact fields).
* Outputs drift classification.

### ✔ Generate Healing Script

The **Healer Agent** produces suggested fixes (e.g., update transformation, modify schema, adjust mappings).

---

## Not Implemented (Future Roadmap)

The following components are designed but **not yet implemented**:

### ❌ Database Crawler Agent

To identify schemas from SQL/NoSQL sources.

### ❌ API Crawler Agent

To infer schema from API payloads, OpenAPI, or sample responses.

### ❌ Notification Agent

Would push drift alerts to communication channels.

### ❌ Dynamic Pipeline-to-Transformation Mapping

The ability to automatically:

* Resolve pipelines affected by a schema change.
* Identify which transformations depend on changed fields.
* Dynamically determine downstream system impact.

---

## Summary

This prototype provides a strong foundation for automated schema drift detection using a modular, agent-driven architecture. The implemented components demonstrate end-to-end functionality—from schema extraction to drift detection and healing—while the remaining planned features will complete the vision of a fully autonomous schema governance system.

---

For questions or contributions, feel free to open an issue or submit a pull request.


# SchemaDriftDetector Crew

Welcome to the SchemaDriftDetector Crew project, powered by [crewAI](https://crewai.com). This template is designed to help you set up a multi-agent AI system with ease, leveraging the powerful and flexible framework provided by crewAI. Our goal is to enable your agents to collaborate effectively on complex tasks, maximizing their collective intelligence and capabilities.

## Installation

Ensure you have Python >=3.10 <3.14 installed on your system. This project uses [UV](https://docs.astral.sh/uv/) for dependency management and package handling, offering a seamless setup and execution experience.

First, if you haven't already, install uv:

```bash
pip install uv
```

Next, navigate to your project directory and install the dependencies:

(Optional) Lock the dependencies and install them by using the CLI command:
```bash
crewai install
```
### Customizing

**Add your `OPENAI_API_KEY` into the `.env` file**

- Modify `src/schema_drift_detector/config/agents.yaml` to define your agents
- Modify `src/schema_drift_detector/config/tasks.yaml` to define your tasks
- Modify `src/schema_drift_detector/crew.py` to add your own logic, tools and specific args
- Modify `src/schema_drift_detector/main.py` to add custom inputs for your agents and tasks

## Running the Project

To kickstart your crew of AI agents and begin task execution, run this from the root folder of your project:

```bash
$ crewai run
```

This command initializes the schema_drift_detector Crew, assembling the agents and assigning them tasks as defined in your configuration.

This example, unmodified, will run the create a `report.md` file with the output of a research on LLMs in the root folder.

## Understanding Your Crew

The schema_drift_detector Crew is composed of multiple AI agents, each with unique roles, goals, and tools. These agents collaborate on a series of tasks, defined in `config/tasks.yaml`, leveraging their collective skills to achieve complex objectives. The `config/agents.yaml` file outlines the capabilities and configurations of each agent in your crew.

## Support

For support, questions, or feedback regarding the SchemaDriftDetector Crew or crewAI.
- Visit our [documentation](https://docs.crewai.com)
- Reach out to us through our [GitHub repository](https://github.com/joaomdmoura/crewai)
- [Join our Discord](https://discord.com/invite/X4JWnZnxPb)
- [Chat with our docs](https://chatg.pt/DWjSBZn)

Let's create wonders together with the power and simplicity of crewAI.
