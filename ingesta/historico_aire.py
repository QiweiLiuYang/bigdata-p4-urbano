"""Carga única del histórico horario de calidad del aire 2016–2022 (portal CKAN de Valencia).

Uso (desde la raíz del repo):
    python ingesta/historico_aire.py            # descarga solo lo que falta
    python ingesta/historico_aire.py --forzar   # vuelve a descargarlo todo
"""
import argparse
import logging
import re
import sys
import time
import unicodedata
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

CKAN_URL = "https://opendata.vlci.valencia.es/api/3/action/datastore_search"
DESTINO = Path("data/raw/calidad_aire/historico")
TAMANO_PAGINA = 32000  # máximo que devuelve el datastore por petición

RECURSOS = {
    "2016_2021": "4be7248b-9597-4017-89af-82a9b6e2382f",
    "2021_2022": "19b5a1a7-5888-4d3e-b69b-3c1436d64e6e",
}

log = logging.getLogger("historico_aire")


def normalizar_columna(nombre: str) -> str:
    """Minúsculas, sin tildes ni espacios: 'PM2.5' -> 'pm2_5', 'Dia de la semana' -> 'dia_de_la_semana'."""
    sin_tildes = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9_]+", "_", sin_tildes.lower().strip())


def get_con_reintentos(params: dict, reintentos: int = 3) -> dict:
    """GET al datastore con reintentos ante errores de red o del servidor."""
    for intento in range(reintentos):
        try:
            r = requests.get(CKAN_URL, params=params, timeout=120)
            r.raise_for_status()
            return r.json()["result"]
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
            if intento == reintentos - 1:
                raise
            time.sleep(10 * (intento + 1))


def descargar_recurso(resource_id: str) -> list[dict]:
    """Descarga todas las filas de un recurso del datastore, página a página."""
    registros, offset = [], 0
    while True:
        resultado = get_con_reintentos({"resource_id": resource_id, "limit": TAMANO_PAGINA, "offset": offset})
        registros += resultado["records"]
        offset += len(resultado["records"])
        log.info("%s: %d / %d filas", resource_id, offset, resultado["total"])
        if not resultado["records"] or offset >= resultado["total"]:
            return registros


def guardar_como_texto(registros: list[dict], destino: Path) -> None:
    """Guarda todas las columnas como texto (sin alterar valores) con los nombres normalizados."""
    columnas = list(registros[0].keys())
    datos = {
        normalizar_columna(c): pa.array([None if r.get(c) is None else str(r[c]) for r in registros], pa.string())
        for c in columnas
    }
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporal = destino.with_suffix(".tmp")
    pq.write_table(pa.table(datos), temporal, compression="snappy")
    temporal.replace(destino)  # si se corta a medias, no queda un fichero incompleto que parezca válido


def main() -> int:
    parser = argparse.ArgumentParser(description="Carga única del histórico de aire 2016–2022")
    parser.add_argument("--forzar", action="store_true", help="vuelve a descargar aunque ya exista")
    args = parser.parse_args()

    ok = True
    for origen, resource_id in RECURSOS.items():
        destino = DESTINO / f"origen={origen}" / "datos.parquet"
        if destino.exists() and not args.forzar:
            log.info("%s ya existe, se omite (usa --forzar para repetir)", destino)
            continue
        try:
            registros = descargar_recurso(resource_id)
            guardar_como_texto(registros, destino)
            log.info("%s: %d filas -> %s", origen, len(registros), destino)
        except Exception:
            log.exception("Fallo al descargar %s", origen)
            ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    sys.exit(main())