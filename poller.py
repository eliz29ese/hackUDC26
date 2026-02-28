# Librería para leer variables de entorno del sistema operativo
import os
# Librería para hacer pausas entre consultas
import time
# Librería para hacer peticiones HTTP a la API de MeteoGalicia
import requests
# Para mostrar la hora actual en los logs de consola
from datetime import datetime
# Para leer el archivo .env y cargar sus valores como variables de entorno
from dotenv import load_dotenv
# Cliente de InfluxDB para conectarse y escribir datos
from influxdb_client import InfluxDBClient, Point
# Modo de escritura síncrono: espera confirmación de InfluxDB antes de continuar
from influxdb_client.client.write_api import SYNCHRONOUS

# Lee el archivo .env y mete sus valores en el entorno del sistema
load_dotenv()

# Recoge cada variable del .env y la guarda como constante Python
METEOSIX_KEY  = os.getenv("METEOSIX_KEY")
INFLUX_URL    = os.getenv("INFLUX_URL")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN")
INFLUX_ORG    = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

# URL del endpoint de la API de MeteoGalicia
BASE_URL = "https://servizos.meteogalicia.gal/mf-meteosix-api/getWeatherInfo"

# Ciudades que se van a consultar con sus coordenadas (longitud, latitud)
# Puedes añadir o quitar ciudades de esta lista
LOCATIONS = [
    {"name": "Santiago", "lon": -8.5448, "lat": 42.8782},
    {"name": "Vigo",     "lon": -8.7207, "lat": 42.2328},
    {"name": "Coruña",   "lon": -8.4115, "lat": 43.3713},
]

# Une los nombres de variables en un string "temperature,wind_module,..."
# que es el formato que espera la API
VARIABLES = ",".join([
    "temperature",
    "wind_module",
    "wind_direction",
    "relative_humidity",
    "precipitation_amount",
    "cloud_area_fraction",
    "air_pressure_at_sea_level",
])

# Segundos entre cada consulta a la API (600 = 10 minutos)
POLL_INTERVAL = 600


def fetch_weather(loc):
    # Construye los parámetros de la petición GET
    params = {
        "lonlat": f"{loc['lon']},{loc['lat']}",  # Coordenadas de la ciudad
        "variables": VARIABLES,                   # Variables que queremos obtener
        "API_KEY": METEOSIX_KEY,                  # Token de autenticación
        "format": "application/json",             # Pedimos la respuesta en JSON
    }
    # Hace la petición GET a la API. Si tarda más de 15s lanza un error
    r = requests.get(BASE_URL, params=params, timeout=15)
    # Si la API devuelve un error (401, 404, 500...) lanza una excepción
    r.raise_for_status()
    # Devuelve la respuesta convertida de JSON a diccionario Python
    return r.json()


def write_to_influx(data, loc_name, write_api):
    # La respuesta de MeteoGalicia es un GeoJSON; los datos están dentro de "features"
    features = data.get("features", [])
    if not features:
        print(f"  [WARN] Sin datos para {loc_name}")
        return

    # Dentro del primer feature, saca la lista de días con predicciones
    days = features[0].get("properties", {}).get("days", [])
    # Contador para el log de consola
    written = 0

    # Recorre cada día de predicción
    for day in days:
        # Recorre cada variable dentro del día (temperatura, viento, etc.)
        for var in day.get("variables", []):
            var_name = var.get("name")
            # Recorre cada valor horario de esa variable
            for val in var.get("values", []):
                ts = val.get("timeInstant")  # Timestamp del valor
                v  = val.get("value")        # Valor numérico
                # Si falta el timestamp o el valor, lo saltamos
                if ts is None or v is None:
                    continue
                try:
                    # Crea un punto de InfluxDB con:
                    # - "weather" como nombre de la medición
                    # - tags: location y variable (para filtrar en Grafana)
                    # - field: el valor numérico real que se almacena
                    # - time: el timestamp del dato
                    point = (
                        Point("weather")
                        .tag("location", loc_name)
                        .tag("variable", var_name)
                        .field("value", float(v))
                        .time(ts)
                    )
                    # Escribe el punto en InfluxDB
                    write_api.write(INFLUX_BUCKET, INFLUX_ORG, point)
                    written += 1
                except (ValueError, TypeError) as e:
                    print(f"  [WARN] No se pudo escribir {var_name}={v}: {e}")

    print(f"  → {loc_name}: {written} puntos escritos")


def main():
    # Abre la conexión con InfluxDB usando las credenciales del .env
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    print(f"Poller iniciado. Intervalo: {POLL_INTERVAL}s | Ciudades: {[l['name'] for l in LOCATIONS]}")

    # Bucle infinito: el script corre hasta que lo matas con Ctrl+C
    while True:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Consultando API...")
        # Consulta cada ciudad de la lista
        for loc in LOCATIONS:
            try:
                data = fetch_weather(loc)
                write_to_influx(data, loc["name"], write_api)
            except requests.HTTPError as e:
                # Error de la API (token inválido, ciudad no encontrada, etc.)
                print(f"  [ERROR] {loc['name']} HTTP {e.response.status_code}: {e}")
            except requests.RequestException as e:
                # Error de red (sin conexión, timeout, etc.)
                print(f"  [ERROR] {loc['name']} red: {e}")
            except Exception as e:
                # Cualquier otro error inesperado
                print(f"  [ERROR] {loc['name']} inesperado: {e}")

        print(f"Próxima consulta en {POLL_INTERVAL}s...")
        # Espera POLL_INTERVAL segundos antes de volver a consultar
        time.sleep(POLL_INTERVAL)


# Solo ejecuta main() si se lanza este archivo directamente
# (no si se importa desde otro script)
if __name__ == "__main__":
    main()
