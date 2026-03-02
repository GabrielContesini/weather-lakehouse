import json
import os
import time
import base64
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

import azure.functions as func
import requests
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

CITIES = [
    {
        "city_id": "3550308",
        "name": "São Paulo",
        "uf": "SP",
        "lat": -23.5505,
        "lon": -46.6333,
    },
    {
        "city_id": "3304557",
        "name": "Rio de Janeiro",
        "uf": "RJ",
        "lat": -22.9068,
        "lon": -43.1729,
    },
    {
        "city_id": "3106200",
        "name": "Belo Horizonte",
        "uf": "MG",
        "lat": -19.9167,
        "lon": -43.9345,
    },
    {
        "city_id": "5300108",
        "name": "Brasília",
        "uf": "DF",
        "lat": -15.7939,
        "lon": -47.8828,
    },
    {
        "city_id": "4106902",
        "name": "Curitiba",
        "uf": "PR",
        "lat": -25.4284,
        "lon": -49.2733,
    },
]

HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
]

CONTAINER = os.getenv("DATALAKE_CONTAINER", "datalake")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "bronze/openmeteo")
WATERMARK_BLOB = os.getenv("WATERMARK_BLOB", "bronze/_meta/weather_watermark.json")
MANIFEST_PREFIX = os.getenv("MANIFEST_PREFIX", "bronze/_meta/manifests")


def _utcnow_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _parse_yyyy_mm_dd(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _blob_service() -> BlobServiceClient:
    conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    api_version = os.getenv("AZURE_STORAGE_API_VERSION")
    if not conn:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING não definido.")
    parts = {}
    for item in conn.split(";"):
        if "=" in item:
            k, v = item.split("=", 1)
            parts[k] = v

    account_name = parts.get("AccountName", "")
    account_key = parts.get("AccountKey", "")
    has_connection_fields = (
        "DefaultEndpointsProtocol" in parts
        or "BlobEndpoint" in parts
        or "AccountName" in parts
        or "SharedAccessSignature" in parts
    )

    if not has_connection_fields:
        raise RuntimeError(
            "AZURE_STORAGE_CONNECTION_STRING inválido: parece que foi informada "
            "apenas a AccountKey. Use a string completa no formato "
            "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
        )

    if account_name.startswith("<") or account_name.endswith(">"):
        raise RuntimeError(
            "AccountName está com placeholder em AZURE_STORAGE_CONNECTION_STRING. "
            "Substitua por um Storage Account real."
        )
    if account_key:
        if account_key.startswith("<") or account_key.endswith(">"):
            raise RuntimeError(
                "AccountKey está com placeholder em AZURE_STORAGE_CONNECTION_STRING. "
                "Substitua pela chave real do Azure Storage."
            )
        try:
            base64.b64decode(account_key, validate=True)
        except Exception as e:
            raise RuntimeError(
                "AccountKey inválida em AZURE_STORAGE_CONNECTION_STRING. "
                "Use a chave completa (base64) em Access keys do Storage Account."
            ) from e
    try:
        if api_version:
            return BlobServiceClient.from_connection_string(
                conn, api_version=api_version
            )
        return BlobServiceClient.from_connection_string(conn)
    except Exception as e:
        raise RuntimeError(
            "AZURE_STORAGE_CONNECTION_STRING inválido. "
            "Use o connection string completo do Azure Storage Account."
        ) from e


def _ensure_container(container_client):
    try:
        container_client.create_container()
    except ResourceExistsError:
        return


def _download_watermark(container_client) -> Dict[str, Any]:
    try:
        blob = container_client.get_blob_client(WATERMARK_BLOB)
        data = blob.download_blob().readall()
        return json.loads(data.decode("utf-8"))
    except Exception:
        return {"last_loaded_date": None}


def _upload_json(container_client, blob_name: str, payload: Dict[str, Any]):
    blob = container_client.get_blob_client(blob_name)
    blob.upload_blob(
        json.dumps(payload, ensure_ascii=False).encode("utf-8"), overwrite=True
    )


def _call_open_meteo(
    lat: float, lon: float, start_date: str, end_date: str
) -> Dict[str, Any]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "UTC",
    }

    last_err = None
    for attempt in range(1, 4):
        try:
            r = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=40)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(2 * attempt)

    raise RuntimeError(f"Falha ao chamar Open-Meteo após retries: {last_err}")


@app.route(route="weather_ingest_http", methods=["GET", "POST"])
def weather_ingest_http(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json() if req.method.upper() == "POST" else {}
    except Exception:
        body = {}

    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    default_date = yesterday.strftime("%Y-%m-%d")

    start_date = str(body.get("start_date") or default_date)
    end_date = str(body.get("end_date") or start_date)

    try:
        _parse_yyyy_mm_dd(start_date)
        _parse_yyyy_mm_dd(end_date)
    except Exception:
        return func.HttpResponse(
            json.dumps({"error": "Datas inválidas. Use YYYY-MM-DD."}),
            status_code=400,
            mimetype="application/json",
        )

    try:
        bsc = _blob_service()
        cc = bsc.get_container_client(CONTAINER)
        _ensure_container(cc)
        run_ts = _utcnow_ts()
        t0 = time.perf_counter()
        started_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        wm_before = _download_watermark(cc)

        written = []
        for c in CITIES:
            try:
                data = _call_open_meteo(c["lat"], c["lon"], start_date, end_date)
                payload = {
                    "run_ts": run_ts,
                    "source": "open-meteo",
                    "city": c,
                    "request": {
                        "start_date": start_date,
                        "end_date": end_date,
                        "hourly": HOURLY_VARS,
                    },
                    "response": data,
                }

                blob_name = f"{BRONZE_PREFIX}/dt={start_date}/city={c['city_id']}/run_ts={run_ts}.json"
                _upload_json(cc, blob_name, payload)
                written.append(
                    {"city_id": c["city_id"], "status": "success", "blob": blob_name}
                )
            except Exception as e:
                written.append(
                    {"city_id": c["city_id"], "status": "failed", "error": str(e)}
                )

        finished_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        duration_ms = int((time.perf_counter() - t0) * 1000)

        cities_success = sum(1 for r in written if r.get("status") == "success")
        cities_failed = sum(1 for r in written if r.get("status") == "failed")
        all_ok = cities_failed == 0

        wm_after = dict(wm_before)
        if all_ok:
            wm_after["last_loaded_date"] = end_date
            wm_after["updated_at_utc"] = run_ts
            _upload_json(cc, WATERMARK_BLOB, wm_after)
        else:
            _upload_json(cc, WATERMARK_BLOB, wm_before)

        manifest = {
            "run_ts": run_ts,
            "requested": {"start_date": start_date, "end_date": end_date},
            "started_at_utc": started_at_utc,
            "finished_at_utc": finished_at_utc,
            "duration_ms": duration_ms,
            "watermark_before": wm_before,
            "watermark_after": wm_after if all_ok else wm_before,
            "summary": {
                "cities_total": len(CITIES),
                "cities_success": cities_success,
                "cities_failed": cities_failed,
            },
            "outputs": written,
        }

        manifest_blob = f"{MANIFEST_PREFIX}/dt={start_date}/run_ts={run_ts}.json"
        _upload_json(cc, manifest_blob, manifest)

        return func.HttpResponse(
            json.dumps(
                {
                    "ok": all_ok,
                    "start_date": start_date,
                    "end_date": end_date,
                    "summary": manifest["summary"],
                    "manifest_blob": manifest_blob,
                    "written": written,
                },
                ensure_ascii=False,
            ),
            status_code=200,
            mimetype="application/json",
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json",
        )
