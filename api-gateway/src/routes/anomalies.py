from fastapi import APIRouter, Query
from src.db import get_client

router = APIRouter(prefix="/api/anomalies", tags=["anomalies"])


@router.get("")
def list_anomalies(
    service: str | None = None,
    minutes: int = Query(1440, ge=1, le=10080),
    limit: int = Query(100, ge=1, le=1000),
):
    client = get_client()
    where = [f"detected_at >= now() - INTERVAL {minutes} MINUTE"]
    if service:
        where.append(f"service = {client.escape_string(service)!r}")
    where_clause = " AND ".join(where)

    result = client.query(
        f"""
        SELECT id, detected_at, window_start, window_end, service,
               metric, value, threshold, summary, suggested_steps
        FROM logs.anomalies
        WHERE {where_clause}
        ORDER BY detected_at DESC
        LIMIT {limit}
        """
    )
    return [dict(zip(result.column_names, row)) for row in result.result_rows]
