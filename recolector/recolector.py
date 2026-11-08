import time
import requests
import csv
import os
import logging
import sys
from dotenv import load_dotenv
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


log = logging.getLogger("recolector")

GEOPORTAL = "https://geoportal.valencia.es/server/rest/services/OPENDATA"

FUENTES = {
    "espiras": {"capa": "Trafico/MapServer/208", "ruta": "trafico/espiras"},
    "tramos": {"capa": "Trafico/MapServer/188", "ruta": "trafico/tramos"},
    "estado": {"capa": "Trafico/MapServer/192", "ruta": "trafico/estado"},
    "aire": {"capa": "MedioAmbiente/MapServer/156", "ruta": "calidad_aire/estaciones"},
    "meteo": {"capa": "MedioAmbiente/MapServer/157", "ruta": "meteo/municipal"},
}

DATA_DIR = Path("data/raw")

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
AEMET_API_KEY = os.getenv("AEMET_API_KEY")

AEMET_URL = "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/diaria/46250"

def get_con_reintentos(url, params, reintentos=3, headers=None):
    """GET con reintentos ante errores de red, 429 y 5xx."""
    for intento in range(reintentos):
        try:
            r = requests.get(url, params=params, timeout=90, headers=headers)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
            if intento == reintentos - 1:
                raise
            time.sleep(10 * (intento + 1))


def descargar_capa(capa: str) -> list[dict]:
    """Devuelve todos los registros de una capa ArcGIS (con paginación)."""
    url = f"{GEOPORTAL}/{capa}/query"
    params = {"where": "1=1", "outFields": "*", "f": "json", "resultOffset": 0}
    registros = []
    while True:
        datos = get_con_reintentos(url, params).json()
        registros += [f["attributes"] for f in datos["features"]]
        if not datos.get("exceededTransferLimit"):
            return registros
        params["resultOffset"] = len(registros)


def descargar_aemet() -> list[dict]:
    """Predicción diaria de Valencia (AEMET, dos pasos). Una fila por día"""
    if not AEMET_API_KEY:
        raise RuntimeError("Falta AEMET_API_KEY en el .env")

    paso1 = get_con_reintentos(AEMET_URL, None, headers={"api_key": AEMET_API_KEY}).json()
    if paso1.get("estado") != 200:
        raise RuntimeError(f"AEMET estado {paso1.get('estado')}: {paso1.get('descripcion')}")

    prediccion = get_con_reintentos(paso1["datos"], None).json()

    registros = []
    for dia in prediccion[0]["prediccion"]["dia"]:
        prob = next(
            (p.get("value") for p in dia.get("probPrecipitacion", [])
            if p.get("periodo", "00-24") == "00-24"),
            None,
        )

        registros.append({
            "fecha": dia["fecha"][:10],
            "temperatura_maxima": dia["temperatura"]["maxima"],
            "temperatura_minima": dia["temperatura"]["minima"],
            "probabilidad_precipitacion": prob,
        })
    return registros



def guardar_parquet(registros: list[dict], ruta: str, nombre: str, captura: datetime) -> Path:
    """Escribe <DATA_DIR>/<ruta>/fecha=AAAA-MM-DD/<nombre>_AAAAMMDDTHHMMSSZ.parquet."""
    tabla = pa.Table.from_pylist(registros)
    tabla = tabla.append_column("captura_utc", pa.array([captura] * tabla.num_rows))
    carpeta = DATA_DIR / ruta / f"fecha={captura:%Y-%m-%d}"
    carpeta.mkdir(parents=True, exist_ok=True)
    destino = carpeta / f"{nombre}_{captura:%Y%m%dT%H%M%SZ}.parquet"
    pq.write_table(tabla, destino, compression="snappy")
    return destino


def registrar(captura: datetime, fuente: str, ok: bool, filas: int, error: str = "") -> None:
    """Añade una línea a _registro/ejecuciones.csv."""
    fichero = DATA_DIR / "_registro" / "ejecuciones.csv"
    fichero.parent.mkdir(parents=True, exist_ok=True)
    nuevo = not fichero.exists()
    with fichero.open("a", newline="") as f:
        w = csv.writer(f)
        if nuevo:
            w.writerow(["captura_utc", "fuente", "ok", "filas", "error"])
        w.writerow([captura.isoformat(), fuente, int(ok), filas, error[:300]])


def recoger(nombre: str) -> None:
    captura = datetime.now(timezone.utc)
    try:
        if nombre == "aemet":
            registros, ruta = descargar_aemet(), "meteo/aemet"
        else:
            registros, ruta = descargar_capa(FUENTES[nombre]["capa"]), FUENTES[nombre]["ruta"]
        destino = guardar_parquet(registros, ruta, nombre, captura)
        registrar(captura, nombre, True, len(registros))
        log.info("%s: %d filas -> %s", nombre, len(registros), destino)
    except Exception as e:
        registrar(captura, nombre, False, 0, str(e))
        log.exception("Fallo en %s", nombre)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    for fuente in sys.argv[1:]:   # p. ej.: python recolector.py espiras tramos aire meteo
        recoger(fuente)