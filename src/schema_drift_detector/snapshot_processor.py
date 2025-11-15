import os
import hashlib
from datetime import datetime
from typing import Dict, Any

from flask import Flask, request, jsonify
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

def get_env(key: str, default=None, required=False):
    value = os.getenv(key, default)
    if required and (value is None or str(value).strip() == ""):
        raise EnvironmentError(f"Environment variable '{key}' is required but not set.")
    return value

NEO4J_URI = get_env("NEO4J_URI", required=True)
NEO4J_USER = get_env("NEO4J_USER", required=True)
NEO4J_PASSWORD = get_env("NEO4J_PASSWORD", required=True)
NEO4J_DB  = get_env("NEO4J_DB", required=True)
CSV_ROOT  = get_env("CSV_ROOT", required=True)
PORT      = get_env("APP_PORT", default=5000, required=False)
SAMPLE_ROW_LIMIT = int(get_env("SAMPLE_ROW_LIMIT", default=5, required=True))  # used only for local inference, not persisted

app = Flask(__name__)
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) 

def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def infer_type(series: pd.Series) -> str:
    """Simple prototype type inference (non-exhaustive)."""
    s = series.dropna()
    if s.empty:
        return "string"
    # date check
    try:
        pd.to_datetime(s, errors="raise", infer_datetime_format=True)
        return "date"
    except Exception:
        pass
    # integer detection
    if all(str(x).lstrip("+-").isdigit() for x in s):
        return "int"
    # float detection
    try:
        _ = s.astype(float)
        return "float"
    except Exception:
        pass
    return "string"

def field_fingerprint(entity_name: str, name: str, dtype: str, nullable: bool) -> str:
    text = f"{entity_name}|{name}|{dtype}|{str(nullable).lower()}"
    return sha1(text)[:12]  # short fingerprint

def read_csv_full(csv_path: str) -> pd.DataFrame:
    """Read full CSV as strings (we will compute stats in-memory but not persist rows)."""
    return pd.read_csv(csv_path, dtype=str)

def create_snapshot_schema_only(job_id: str, entity_name: str, csv_path: str) -> Dict[str, Any]:
    """
    Read CSV (in-memory), compute schema metadata and aggregated stats,
    persist Snapshot node and Field nodes (no sample/data rows).
    """
    df = read_csv_full(csv_path)
    row_count = len(df)

    timestamp = datetime.utcnow().isoformat(timespec="seconds")
    snapshot_id = f"snapshot_{job_id}_{timestamp.replace(':', '-')}"  # safe id

    # Build fields metadata (no examples)
    fields_meta = []
    for ordinal, col in enumerate(df.columns):
        series = df[col]
        dtype = infer_type(series)
        nullable = bool(series.isna().any() or any(str(x).strip() == "" for x in series))
        null_count = int(series.isna().sum()) + int((series.fillna("").astype(str) == "").sum())
        unique_count = int(series.nunique(dropna=True))
        distinct_ratio = round(unique_count / row_count, 6) if row_count > 0 else 0.0
        fingerprint = field_fingerprint(entity_name, col, dtype, nullable)
        field_id = f"{entity_name}|{col}|{dtype}|{str(nullable).lower()}"

        fields_meta.append({
            "field_id": field_id,
            "name": col,
            "data_type": dtype,
            "nullable": nullable,
            "ordinal": ordinal,
            "null_count": null_count,
            "unique_count": unique_count,
            "distinct_ratio": distinct_ratio,
            "fingerprint": fingerprint
        })

    # Persist to Neo4j in one session (multiple txs)
    with driver.session(database=NEO4J_DB) as session:
        session.execute_write(_merge_entity, entity_name, csv_path)
        session.execute_write(_merge_snapshot, snapshot_id, entity_name, timestamp, row_count, job_id)
        for f in fields_meta:
            session.execute_write(_merge_field_stats_and_link, entity_name, f)
        session.execute_write(_link_previous_snapshot, entity_name, snapshot_id)

    return {
        "snapshot_id": snapshot_id,
        "entity": entity_name,
        "csv": csv_path,
        "row_count": row_count,
        "fields_count": len(fields_meta)
    }

# ---- Neo4j tx functions ----
def _merge_entity(tx, entity_name: str, source_path: str):
    cypher = """
    MERGE (e:Entity {name:$entity_name})
      ON CREATE SET e.type='file', e.source_path=$source_path, e.created = datetime()
      ON MATCH SET e.last_seen = datetime()
    """
    tx.run(cypher, entity_name=entity_name, source_path=source_path)

def _merge_snapshot(tx, snapshot_id: str, entity_name: str, timestamp: str, row_count: int, job_id: str):
    cypher = """
    MERGE (s:Snapshot {id:$snapshot_id})
      ON CREATE SET s.source = $entity_name, s.timestamp = datetime($timestamp), s.version = 'v1', s.row_count = $row_count, s.job_id = $job_id
      ON MATCH SET s.timestamp = datetime($timestamp), s.row_count = $row_count
    WITH s
    MATCH (e:Entity {name:$entity_name})
    MERGE (s)-[:HAS_ENTITY]->(e)
    """
    tx.run(cypher, snapshot_id=snapshot_id, entity_name=entity_name, timestamp=timestamp, row_count=row_count, job_id=job_id)

def _merge_field_stats_and_link(tx, entity_name: str, f: Dict[str, Any]):
    """
    Upsert Field node and set aggregated stats (NO sample values).
    Field properties include: null_count, unique_count, distinct_ratio, fingerprint
    """
    cypher = """
    MERGE (fld:Field {field_id:$field_id})
      ON CREATE SET fld.name=$name, fld.data_type=$data_type, fld.nullable=$nullable, fld.ordinal=$ordinal,
                    fld.null_count=$null_count, fld.unique_count=$unique_count, fld.distinct_ratio=$distinct_ratio, fld.fingerprint=$fingerprint
      ON MATCH SET fld.data_type=$data_type, fld.nullable=$nullable, fld.ordinal=$ordinal,
                   fld.null_count=$null_count, fld.unique_count=$unique_count, fld.distinct_ratio=$distinct_ratio, fld.fingerprint=$fingerprint
    WITH fld
    MATCH (e:Entity {name:$entity_name})
    MERGE (e)-[:HAS_FIELD]->(fld)
    """
    tx.run(cypher,
           field_id=f["field_id"],
           name=f["name"],
           data_type=f["data_type"],
           nullable=f["nullable"],
           ordinal=int(f["ordinal"]),
           null_count=int(f["null_count"]),
           unique_count=int(f["unique_count"]),
           distinct_ratio=float(f["distinct_ratio"]),
           fingerprint=f["fingerprint"],
           entity_name=entity_name)
    
def _link_previous_snapshot(tx, entity_name: str, new_snapshot_id: str):
    # Link most recent previous snapshot to this snapshot via PRECEDES
    cypher = """
    MATCH (s:Snapshot)-[:HAS_ENTITY]->(e:Entity {name:$entity_name})
    WHERE s.id <> $new_snapshot_id
    RETURN s.id AS id, s.timestamp AS ts
    ORDER BY s.timestamp DESC
    LIMIT 1
    """
    rec = tx.run(cypher, entity_name=entity_name, new_snapshot_id=new_snapshot_id).single()
    if rec:
        prev_id = rec["id"]
        tx.run("""
            MATCH (prev:Snapshot {id:$prev_id}), (next:Snapshot {id:$next_id})
            MERGE (prev)-[:PRECEDES]->(next)
        """, prev_id=prev_id, next_id=new_snapshot_id)

def get_source_for_job(job_id: str) -> Dict[str, str]:
    q = """
    MATCH (job:ETLJob {job_id:$job_id})-[:USES_SOURCE]->(e:Entity)
    RETURN e.name AS entity_name, e.source_path AS source_path
    LIMIT 1
    """
    with driver.session(database=NEO4J_DB) as session:
        rec = session.execute_read(lambda tx: tx.run(q, job_id=job_id).single())
        if not rec:
            raise ValueError(f"ETL job '{job_id}' not found or has no USES_SOURCE relationship.")
        return {"entity_name": rec["entity_name"], "source_path": rec["source_path"]}

# ---- Flask endpoint ----
@app.route("/snapshot", methods=["POST"])
def snapshot_endpoint():
    """
    API: /snapshot?job=<job_id>
    Example: POST /snapshot?job=job_company_v1
    """
    job_id = request.args.get("job")
    if not job_id:
        return jsonify({"error": "job parameter is required (e.g. job_company_v1)"}), 400

    try:
        info = get_source_for_job(job_id)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404

    entity_name = info["entity_name"]
    source_path = info["source_path"] or f"{entity_name}"
    csv_file = os.path.join(CSV_ROOT, source_path)
    if not os.path.exists(csv_file):
        return jsonify({"error": f"CSV file not found at path: {csv_file}"}), 400

    try:
        result = create_snapshot_schema_only(job_id, entity_name, csv_file)
    except Exception as ex:
        return jsonify({"error": f"Failed to create snapshot: {ex}"}), 500

    return jsonify(result), 201

if __name__ == "__main__":
    app.run(debug=True, port=int(PORT))