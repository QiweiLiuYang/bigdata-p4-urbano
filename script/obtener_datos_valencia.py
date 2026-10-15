import os
import json
import requests
from datetime import date
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://geoportal.valencia.es/server/rest/services/OPENDATA/Trafico/MapServer/188/query?where=1=1&outFields=*&f=json"
DESTINATION = Path(os.getenv("DESTINATION_RAW"))

def obtener_datos() -> list[dict]:
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()  # lanza excepción si el código HTTP no es 2xx
    return resp.json()

if __name__ == "__main__":
    registros = obtener_datos()
    fecha = date.today().isoformat()
    ruta_salida = f"{DESTINATION}/intensidad_trafico_valencia_{fecha}.json"
    os.makedirs(DESTINATION, exist_ok=True)
    with open(ruta_salida, "w", encoding="utf-8") as f:
        json.dump(registros, f, ensure_ascii=False, indent=2)
    print(f"Ingesta completada: {len(registros)} registros guardados en {ruta_salida}")