from fastapi import APIRouter, Query
from src.db import get_client

router = APIRouter(prefix="/api/logs", tags=["logs"])


@router.get("")
def list_logs(
    service: str | None = None,
    severity: str | None = None,
    minutes: int = Query(30, ge=1, le=1440),
    limit: int = Query(200, ge=1, le=2000),
):
    client = get_client()
    where = [f"ts >= now() - INTERVAL {minutes} MINUTE"]
    if service:
        where.append(f"service = {client.escape_string(service)!r}")
    if severity:
        where.append(f"severity = {client.escape_string(severity)!r}")
    where_clause = " AND ".join(where)

    result = client.query(
        f"""
        SELECT ts, service, severity, message, latency_ms, trace_id
        FROM logs.raw_logs
        WHERE {where_clause}
        ORDER BY ts DESC
        LIMIT {limit}
        """
    )
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


@router.get("/error-rate-timeseries")
def error_rate_timeseries(minutes: int = Query(60, ge=1, le=1440), bucket_seconds: int = 30):
    client = get_client()
    result = client.query(
        f"""
        SELECT
            toStartOfInterval(ts, INTERVAL {bucket_seconds} SECOND) AS bucket,
            service,
            countIf(severity = 'ERROR') AS errors,
            count() AS total
        FROM logs.raw_logs
        WHERE ts >= now() - INTERVAL {minutes} MINUTE
        GROUP BY bucket, service
        ORDER BY bucket ASC
        """
    )
    rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
    for r in rows:
        r["error_rate"] = (r["errors"] / r["total"]) if r["total"] else 0
    return rows
