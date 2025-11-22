import csv
import os
import logging
import datetime
import io
from typing import Any, Dict, List, Optional

DEFAULT_MAX_BYTES = 32 * 1024  # 32 KiB

logger = logging.getLogger("csv_crawler_agent")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


# Conservative field type inference (metadata-only)
def infer_type_from_name(col_name: str) -> str:
    """
    Heuristic, conservative inference based on column name only.
    Returns: 'string' | 'int' | 'float' | 'date'
    Keep this conservative — avoid data sampling.
    """
    n = col_name.lower()
    if any(tok in n for tok in ("date", "dob", "timestamp", "ts", "joined", "birth")):
        return "date"
    if n.endswith("_id") or n == "id" or any(tok in n for tok in ("country_code")):
        # age sometimes computed; treat as int
        return "string"
    if n.endswith("_id") or n == "id" or any(tok in n for tok in ("num", "age", "count", "quantity", "year")):
        # age sometimes computed; treat as int
        return "int"
    if any(tok in n for tok in ("amount", "price", "cost", "total", "balance")):
        return "float"
    # fallback
    return "string"

def _parse_header_from_bytes(b: bytes, encoding: str = "utf-8") -> List[str]:
    """Return first non-empty CSV row parsed from bytes using csv.reader."""
    text = b.decode(encoding, errors="replace")
    sio = io.StringIO(text)
    reader = csv.reader(sio)
    for row in reader:
        if row:
            return [col.strip() for col in row]
    return []


def _normalize_field_name(name: str) -> str:
    # Trim and preserve original; do small normalisation
    return name.strip()


def _resolve_source_descriptor(metadata_ref: Dict[str, Any], source_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Normalize metadata_ref and source_id into a descriptor dict:
      { "scheme": "local"|"s3"|"gs"|"azure"|"http", ... }
    This function does not fetch credentials.
    """
    # If metadata_ref contains 'properties', prefer it
    props = {}
    if isinstance(metadata_ref, dict):
        props = metadata_ref.get("properties", metadata_ref)
    # candidate keys
    keys = ("source_path", "path", "file_path", "path_template", "s3_path", "gs_path", "url", "azure_path")
    for k in keys:
        if isinstance(props, dict) and props.get(k):
            p = str(props.get(k)).strip()
            if p.startswith("s3://"):
                _, rest = p.split("s3://", 1)
                bucket, _, key = rest.partition("/")
                return {"scheme": "s3", "bucket": bucket, "key": key, "raw": p}
            if p.startswith("gs://"):
                _, rest = p.split("gs://", 1)
                bucket, _, key = rest.partition("/")
                return {"scheme": "gs", "bucket": bucket, "key": key, "raw": p}
            if p.startswith("http://") or p.startswith("https://"):
                return {"scheme": "http", "url": p, "raw": p}
            if p.startswith("azure://") or p.startswith("https://") and "blob.core.windows.net" in p:
                # Accept full azure blob URL
                return {"scheme": "azure", "url": p, "raw": p}
            # treat as local path
            p_abs = p if os.path.isabs(p) else os.path.abspath(p)
            return {"scheme": "local", "path": p_abs, "raw": p_abs}

    # fallback: interpret source_id if it's path-like
    if isinstance(source_id, str) and source_id:
        s = source_id.strip()
        if s.startswith("s3://"):
            _, rest = s.split("s3://", 1)
            bucket, _, key = rest.partition("/")
            return {"scheme": "s3", "bucket": bucket, "key": key, "raw": s}
        if s.startswith("gs://"):
            _, rest = s.split("gs://", 1)
            bucket, _, key = rest.partition("/")
            return {"scheme": "gs", "bucket": bucket, "key": key, "raw": s}
        if s.startswith("http://") or s.startswith("https://"):
            return {"scheme": "http", "url": s, "raw": s}
        # local path fallback
        p_abs = s if os.path.isabs(s) else os.path.abspath(s)
        return {"scheme": "local", "path": p_abs, "raw": p_abs}

    return None


def iso_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


# Keys that must NOT be present in snapshots (no rows / PII)
FORBIDDEN_SNAPSHOT_KEYS = {"sample_rows", "rows", "data", "example_values", "example", "samples"}


def _ensure_no_forbidden(snapshot: Dict[str, Any]):
    def _scan(o: Any):
        if isinstance(o, dict):
            for k, v in o.items():
                if k in FORBIDDEN_SNAPSHOT_KEYS:
                    raise ValueError(f"Forbidden key found in snapshot: {k}")
                _scan(v)
        elif isinstance(o, (list, tuple, set)):
            for itm in o:
                _scan(itm)
    _scan(snapshot)

def _read_header(descriptor: Dict[str, Any], header_rows: int = 1, max_bytes: int = DEFAULT_MAX_BYTES) -> List[str]:
    """
    Read first max_bytes bytes from different backends and parse header row(s).
    Returns list of header column names (first non-empty CSV row).
    """
    scheme = descriptor.get("scheme")
    if scheme == "local":
        path = descriptor.get("path")
        if not path or not os.path.exists(path):
            raise FileNotFoundError(f"Local path not found: {path}")
        with open(path, "rb") as fh:
            b = fh.read(max_bytes)
        return _parse_header_from_bytes(b)

    """
    if scheme == "http":
        url = descriptor.get("url")
        if requests is None:
            raise RuntimeError("requests is required for HTTP backend. Install: pip install requests")
        headers = {"Range": f"bytes=0-{max_bytes-1}"}
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        return _parse_header_from_bytes(r.content)
    */

    if scheme == "s3":
        if boto3 is None:
            raise RuntimeError("boto3 is required for S3 backend. Install: pip install boto3")
        bucket = descriptor.get("bucket")
        key = descriptor.get("key")
        if not bucket or key is None:
            raise ValueError("S3 descriptor missing bucket/key")
        s3 = boto3.client("s3")
        try:
            resp = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{max_bytes-1}")
            stream = resp["Body"].read()
            return _parse_header_from_bytes(stream)
        except ClientError as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            # fallback to full get if range unsupported (cautious)
            if code in ("InvalidRange", "RequestedRangeNotSatisfiable"):
                resp = s3.get_object(Bucket=bucket, Key=key)
                body = resp["Body"].read(max_bytes)
                return _parse_header_from_bytes(body)
            raise

    if scheme == "gs" or scheme == "gcs":
        if gcs_storage is None:
            raise RuntimeError("google-cloud-storage is required for GCS backend. Install: pip install google-cloud-storage")
        bucket = descriptor.get("bucket")
        key = descriptor.get("key")
        if not bucket or key is None:
            raise ValueError("GCS descriptor missing bucket/key")
        client = gcs_storage.Client()
        bucket_obj = client.bucket(bucket)
        blob = bucket_obj.blob(key)
        # download first bytes
        # blob.download_as_bytes supports start/end
        b = blob.download_as_bytes(start=0, end=max_bytes - 1)
        return _parse_header_from_bytes(b)

    if scheme == "azure":
        if BlobServiceClient is None:
            raise RuntimeError("azure-storage-blob is required for Azure backend. Install: pip install azure-storage-blob")
        # descriptor may contain full URL or container/name
        url = descriptor.get("url")
        path = descriptor.get("path")
        # If full URL provided, we can do a range GET by requesting URL with requests if public,
        # otherwise use BlobServiceClient with credential from env.
        if url:
            # try using requests range get (public blobs)
            if requests is None:
                raise RuntimeError("requests required to fetch Azure blob by URL. Install: pip install requests")
            headers = {"Range": f"bytes=0-{max_bytes-1}"}
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            return _parse_header_from_bytes(r.content)
        if path:
            # path might be like "/container/blobpath" or "container/blobpath"
            p = path.lstrip("/")
            container, _, blob_name = p.partition("/")
            if not container or not blob_name:
                raise ValueError("Azure path must include container/blob (container/blobname)")
            # Use BlobServiceClient default credential resolution
            client = BlobServiceClient.from_connection_string(os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")) if os.environ.get("AZURE_STORAGE_CONNECTION_STRING") else BlobServiceClient(account_url=f"https://{os.environ.get('AZURE_STORAGE_ACCOUNT')}.blob.core.windows.net")
            container_client = client.get_container_client(container)
            blob_client = container_client.get_blob_client(blob_name)
            stream = blob_client.download_blob(offset=0, length=max_bytes)
            b = stream.readall()
            return _parse_header_from_bytes(b)

    raise ValueError(f"Unsupported or malformed descriptor/scheme: {descriptor}")
"""

class CSVCrawlerAgent:
    """
    Crawl CSV/flat-file sources (header rows, schema hints, metadata) and return a canonical schema snapshot.
    Important: must not read data rows or persist any secrets.
    """

    def __init__(self):
        # nothing to initialise for local file prototype
        pass

    def run(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        request_id = inputs.get("request_id")
        source_id = inputs.get("source_id")
        entity = inputs.get("entity") or source_id
        source_type = inputs.get("source_type")
        metadata_ref = inputs.get("metadata_ref") or {}
        options = inputs.get("options") or {}
        header_rows = int(options.get("header_rows", 1))
        max_bytes = int(options.get("max_bytes", DEFAULT_MAX_BYTES))

        logger.info("CSV Crawler start request_id=%s source_id=%s entity=%s source_type=%s", request_id, source_id, entity, source_type)

        # Skip if source type is not csv/file/text
        if source_type and source_type.lower() not in ["csv", "file", "text"]:
            logger.info("Source type is '%s', not csv/file/text. Skipping CSV crawler.", source_type)
            return {
                "request_id": request_id,
                "source_id": source_id,
                "entity": entity,
                "skipped": True,
                "reason": f"Source type is '{source_type}', not csv/file/text"
            }

        descriptor = _resolve_source_descriptor(metadata_ref, source_id)
        if not descriptor:
            msg = "Could not resolve source descriptor from metadata_ref or source_id"
            logger.error(msg + " metadata_ref=%s source_id=%s", metadata_ref, source_id)
            raise ValueError(msg)

        logger.info("Resolved descriptor: %s", {k: v for k, v in descriptor.items() if k != "raw"})

        # Read header (generic across transports)
        headers = _read_header(descriptor, header_rows=header_rows, max_bytes=max_bytes)
        if not headers:
            logger.warning("No header row found for source=%s descriptor=%s", source_id, descriptor)

        # Build canonical fields list (no sample values)
        fields: List[Dict[str, Any]] = []
        for idx, raw in enumerate(headers):
            name = _normalize_field_name(raw)
            ftype = infer_type_from_name(name)
            # We cannot know nullability without sampling; default to True (conservative)
            nullable = True
            hints = {"inferred_from": "name_heuristic"}
            fields.append({"name": name, "type": ftype, "nullable": nullable, "ordinal": idx, "hints": hints})

        resolved_path = descriptor.get("raw") or descriptor.get("path") or descriptor.get("url") or descriptor.get("bucket")
        snapshot = {
            "source_id": source_id,
            "entity": entity,
            "schema": {
                "fields": fields,
                "version_meta": {
                    "created_by": "csv_crawler_agent",
                    "timestamp": iso_now(),
                    "source_path": resolved_path
                }
            }
        }

        # Safety check
        _ensure_no_forbidden(snapshot)

        result = {
            "request_id": request_id,
            "source_id": source_id,
            "entity": entity,
            "snapshot": snapshot
        }

        logger.info("CSV Crawler complete request_id=%s source_id=%s fields=%d", request_id, source_id, len(fields))
        return result


# Local quick-run for dev testing
if __name__ == "__main__":
    import json, sys
    agent = CSVCrawlerAgent()
    # Example: pass path via metadata_ref or rely on source_id as filename in cwd
    sample_input = {
        "request_id": "local-test-2",
        "source_id": "people-info.csv",
        "entity": "people-info.csv",
        "metadata_ref": {"properties": {"source_path": "/Users/ayanbhattacharyya/Documents/ai-workspace/schema_drift_detector/schema_drift_detector/examples/people-info.csv"}},
        "options": {"header_rows": 1}
    }
    try:
        out = agent.run(sample_input)
        print(json.dumps(out, indent=2))
    except Exception as e:
        print("ERROR:", str(e), file=sys.stderr)
        raise
