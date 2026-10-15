import os
import json
import requests
from datetime import date
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

API_KEY = os.getenv("AEMET_API_KEY")
API_URL = f"https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/diaria/46169/?api_key={API_KEY}"
DESTINATION = Path(os.getenv("DESTINATION_RAW"))

def obtener_datos() -> list[dict]:
    """AEMET responde con un JSON cuyo campo 'datos' es la URL con los datos reales."""
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()  # lanza excepción si el código HTTP no es 2xx
    url_datos = resp.json()["datos"]
    resp = requests.get(url_datos, timeout=30)
    resp.raise_for_status()
    return resp.json()

if __name__ == "__main__":
    registros = obtener_datos()
    fecha = date.today().isoformat()
    ruta_salida = f"{DESTINATION}/aemet_valencia_{fecha}.json"
    os.makedirs(DESTINATION, exist_ok=True)
    with open(ruta_salida, "w", encoding="utf-8") as f:
        json.dump(registros, f, ensure_ascii=False, indent=2)
    print(f"Ingesta completada: {len(registros)} registros guardados en {ruta_salida}")