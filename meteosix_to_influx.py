import os
import json
import time
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

import requests
from dateutil import parser as dtparser

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


BASE_URL = "https://servizos.meteogalicia.gal/apiv5/" # :contentReference[oaicite:6]{index=6}
FIND_PLACES_ENDPOINT = f"{BASE_URL}/findPlaces"
FORECAST_ENDPOINT = f"{BASE_URL}/getNumericForecastInfo"

API_KEY = os.getenv("METEOSIX_API_KEY")  # :contentReference[oaicite:7]{index=7}

INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "hackudc")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "meteosix")

STATE_PATH = os.getenv("ETL_STATE_PATH", "etl_state.json")
PLACES_PATH = os.getenv("PLACES_PATH", "places.json")

MAX_IDS_PER_REQUEST = 20  # 

DEFAULT_VARIABLES = [
    "temperature",
    "relative_humidity",
    "air_pressure_at_sea_level",
    "precipitation_amount",
    "cloud_area_fraction",
    "wind",
    # playa/surf (si queréis, ojo: en interior puede venir sin datos)
    "significative_wave_height",
    "relative_peak_period",
    "mean_wave_direction",
    "sea_water_temperature",
]

DEFAULT_PARAMS = {
    "lang": "es",
    "format": "application/json",  # GeoJSON 
    "exceptionsFormat": "application/json",
}


# ------------------ utils ------------------

def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def chunked(xs: List[str], n: int) -> List[List[str]]:
    return [xs[i:i+n] for i in range(0, len(xs), n)]

def stable_hash(obj: Any) -> str:
    raw = json.dumps(obj, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

def normalize_query(q: str) -> str:
    return q.strip().lower()

# ------------------ 1) findPlaces ------------------

def find_places(query: str, types: str = "locality") -> Dict[str, Any]:
    """
    /findPlaces busca lugares por cadena. params: API_KEY (oblig), location (oblig), types (opcional), lang/format... 
    """
    if not API_KEY:
        raise RuntimeError("Falta METEOSIX_API_KEY en variables de entorno.")

    params = {
        "API_KEY": API_KEY,
        "location": query,           # obligatorio :contentReference[oaicite:11]{index=11}
        "types": types,              # locality/beach :contentReference[oaicite:12]{index=12}
        "lang": "es",
        "format": "application/json",
        "exceptionsFormat": "application/json",
    }
    r = requests.get(FIND_PLACES_ENDPOINT, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if isinstance(data, dict) and "exception" in data:
        raise RuntimeError(f"Error findPlaces: {data['exception']}")
    return data

def pick_best_feature(query: str, features: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Heurística simple:
    1) match exacto por name (case-insensitive)
    2) si no, el primero devuelto (la API devuelve coincidencias por criterio de texto) :contentReference[oaicite:13]{index=13}
    """
    q = normalize_query(query)
    exact = []
    for f in features:
        props = (f.get("properties") or {})
        name = (props.get("name") or "")
        if normalize_query(name) == q:
            exact.append(f)
    if exact:
        return exact[0]
    return features[0] if features else None

def resolve_place_ids(place_queries: List[str], types: str = "locality") -> Dict[str, Dict[str, Any]]:
    """
    Devuelve mapping:
      query -> {id, name, municipality, province, type}
    y lo cachea en places.json
    """
    cache: Dict[str, Dict[str, Any]] = load_json(PLACES_PATH, default={})
    out: Dict[str, Dict[str, Any]] = dict(cache)

    for q in place_queries:
        key = normalize_query(q)

        if key in out and out[key].get("id"):
            continue

        data = find_places(q, types=types)
        features = data.get("features") or []
        best = pick_best_feature(q, features)

        if not best:
            print(f"[WARN] No encuentro lugar para: {q}")
            out[key] = {"id": None, "query": q}
            continue

        props = best.get("properties") or {}
        place_id = (props.get("id") or "").strip()

        out[key] = {
            "query": q,
            "id": place_id,
            "name": props.get("name"),
            "municipality": props.get("municipality"),
            "province": props.get("province"),
            "type": props.get("type"),
        }
        print(f"[OK] {q} -> id={place_id} ({out[key]['name']}, {out[key]['province']})")

        time.sleep(0.2)  # suave

    save_json(PLACES_PATH, out)
    return out

# ------------------ 2) forecast ------------------

def request_forecast(location_ids: List[str], variables: List[str]) -> Dict[str, Any]:
    """
    /getNumericForecastInfo: requiere API_KEY y (locationIds o coords), pero no ambos.
    Límite 20 puntos. 
    """
    if not API_KEY:
        raise RuntimeError("Falta METEOSIX_API_KEY en variables de entorno.")

    params = dict(DEFAULT_PARAMS)
    params["API_KEY"] = API_KEY
    params["locationIds"] = ",".join(location_ids)
    params["variables"] = ",".join(variables)

    r = requests.get(FORECAST_ENDPOINT, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    if isinstance(data, dict) and "exception" in data:
        raise RuntimeError(f"Error global getNumericForecastInfo: {data['exception']}")
    return data

# ------------------ 3) parse + influx ------------------

def iter_timeseries_points(feature: Dict[str, Any]) -> List[Tuple[datetime, Dict[str, Any], Dict[str, Any]]]:
    props = feature.get("properties") or {}

    # En JSON, si un punto falla, la excepción viene dentro del Feature 
    if feature.get("exception"):
        return []

    place_id = (props.get("id") or "").strip()
    place_name = props.get("name") or ""
    municipality = props.get("municipality") or ""
    province = props.get("province") or ""
    place_type = props.get("type") or ""

    days = props.get("days") or []
    out = []

    for day in days:
        variables = day.get("variables") or []
        for var in variables:
            var_name = var.get("name") or ""
            model = var.get("model") or ""
            grid = var.get("grid") or ""
            units = var.get("units") or ""

            tags = {
                "place_id": place_id,
                "place_name": place_name,
                "municipality": municipality,
                "province": province,
                "type": place_type,
                "variable": var_name,
                "model": model,
                "grid": grid,
                "units": units,
            }

            values = var.get("values") or []
            for hv in values:
                ti = hv.get("timeInstant")
                if not ti:
                    continue
                ts = dtparser.isoparse(ti)  # formato yyyy-MM-ddTHH:mm:ssZZ :contentReference[oaicite:16]{index=16}

                fields: Dict[str, Any] = {}

                if var_name == "wind":
                    # wind devuelve módulo y dirección :contentReference[oaicite:17]{index=17}
                    mv = hv.get("moduleValue")
                    dv = hv.get("directionValue")
                    if mv is not None:
                        fields["wind_module"] = float(mv)
                    if dv is not None:
                        fields["wind_direction"] = float(dv)
                else:
                    v = hv.get("value")
                    if v is not None and v != "":
                        try:
                            fields["value"] = float(v)
                        except (ValueError, TypeError):
                            fields["value_str"] = str(v)

                mr = hv.get("modelRun")
                if mr:
                    fields["model_run"] = mr

                if fields:
                    out.append((ts, fields, tags))

    return out

def write_to_influx(points: List[Tuple[datetime, Dict[str, Any], Dict[str, Any]]]) -> int:
    if not (INFLUX_TOKEN and INFLUX_ORG and INFLUX_BUCKET):
        raise RuntimeError("Faltan INFLUX_TOKEN/INFLUX_ORG/INFLUX_BUCKET en env vars.")

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    influx_points = []
    for ts, fields, tags in points:
        p = Point("forecast_hourly").time(ts, WritePrecision.S)
        for k, v in tags.items():
            p = p.tag(k, str(v or ""))
        for fk, fv in fields.items():
            p = p.field(fk, fv)
        influx_points.append(p)

    if influx_points:
        write_api.write(bucket=INFLUX_BUCKET, record=influx_points)

    client.close()
    return len(influx_points)

def run_etl(location_ids: List[str], variables: Optional[List[str]] = None) -> None:
    variables = variables or DEFAULT_VARIABLES
    state: Dict[str, Any] = load_json(STATE_PATH, default={})

    total_written = 0
    for batch in chunked(location_ids, MAX_IDS_PER_REQUEST):
        data = request_forecast(batch, variables)

        batch_key = ",".join(batch)
        h = stable_hash(data)
        if state.get(batch_key) == h:
            print(f"[SKIP] Batch sin cambios: {batch_key}")
            continue

        features = data.get("features") or []
        all_points = []
        skipped = 0

        for feat in features:
            if feat.get("exception"):
                skipped += 1
                continue
            all_points.extend(iter_timeseries_points(feat))

        written = write_to_influx(all_points)
        total_written += written

        state[batch_key] = h
        save_json(STATE_PATH, state)

        print(f"[OK] Batch {batch_key}: escritos {written} puntos; features con exception: {skipped}")
        time.sleep(0.5)

    print(f"TOTAL escritos en Influx: {total_written}")

# ------------------ main ------------------

if __name__ == "__main__":
    PLACE_QUERIES = [
        "a coruña",
        "vigo",
        "santiago de compostela",
        "ferrol",
        "pontevedra",
        "lugo",
        "ourense",
        "baiona",
        "ribeira",
        "cedeira",
        "fisterra",
        "malpica",
        "sanxenxo",
        "cambados",
        "viveiro",
        "o grove",
        "cangas",
        "mondonedo",
        "a guarda",
        "xinzo",
    ]

    # 1) Resolver nombres -> ids usando findPlaces (types=locality)
    places = resolve_place_ids(PLACE_QUERIES, types="locality")

    # 2) Sacar los ids válidos
    LOCATION_IDS = [p["id"] for p in places.values() if p.get("id")]
    if not LOCATION_IDS:
        raise SystemExit("No se resolvió ningún locationId. Revisa METEOSIX_API_KEY o los nombres.")

    # 3) ETL a Influx
    run_etl(LOCATION_IDS)