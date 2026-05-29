"""
Pipeline ETL Automatizado en Python
Extracción y Procesamiento de Datos desde la API REST Countries

Autores:
    - Freddy Santiago Hernández Gelves
    - Edwuar Andrés Niño Portilla
    - Raúl Andrés Vesga Neira

Descripción:
    Este script implementa un pipeline ETL (Extract, Transform, Load)
    que extrae datos de países desde la API pública REST Countries,
    los transforma con Pandas y los almacena en un archivo CSV.
"""

import requests
import pandas as pd
import logging
import os
from datetime import datetime

# ─── Configuración del logger ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Constantes ──────────────────────────────────────────────────────────────
API_URL = "https://restcountries.com/v3.1/all"
OUTPUT_DIR = "output"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "countries_data.csv")


# ─── ETAPA 1: EXTRACCIÓN ─────────────────────────────────────────────────────
def extract(url: str) -> list[dict]:
    """
    Extrae datos en formato JSON desde la API pública REST Countries.

    Args:
        url: URL del endpoint de la API.

    Returns:
        Lista de diccionarios con los datos crudos de cada país.

    Raises:
        requests.HTTPError: Si la respuesta de la API no es exitosa.
    """
    logger.info("Iniciando extracción de datos desde: %s", url)
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    data = response.json()
    logger.info("Extracción exitosa: %d registros obtenidos.", len(data))
    return data


# ─── ETAPA 2: TRANSFORMACIÓN ─────────────────────────────────────────────────
def transform(raw_data: list[dict]) -> pd.DataFrame:
    """
    Transforma los datos crudos: selección de campos, limpieza,
    normalización de columnas y conversión de tipos.

    Args:
        raw_data: Lista de diccionarios con los datos crudos.

    Returns:
        DataFrame limpio y normalizado listo para cargar.
    """
    logger.info("Iniciando transformación de datos...")

    registros = []
    for pais in raw_data:
        registro = {
            "nombre_comun":      pais.get("name", {}).get("common"),
            "nombre_oficial":    pais.get("name", {}).get("official"),
            "region":            pais.get("region"),
            "subregion":         pais.get("subregion"),
            "capital":           pais.get("capital", [None])[0],
            "poblacion":         pais.get("population"),
            "area_km2":          pais.get("area"),
            "codigo_alpha2":     pais.get("cca2"),
            "codigo_alpha3":     pais.get("cca3"),
            "independiente":     pais.get("independent"),
            "pais_sin_salida_mar": pais.get("landlocked"),
            "moneda":            _extraer_moneda(pais.get("currencies", {})),
            "idiomas":           _extraer_idiomas(pais.get("languages", {})),
            "continentes":       ", ".join(pais.get("continents", [])),
        }
        registros.append(registro)

    df = pd.DataFrame(registros)

    # --- Limpieza ---
    registros_antes = len(df)
    df.dropna(subset=["nombre_comun", "region"], inplace=True)
    logger.info(
        "Registros eliminados por valores nulos en campos clave: %d",
        registros_antes - len(df),
    )

    # --- Normalización de columnas de texto ---
    cols_texto = ["nombre_comun", "nombre_oficial", "region", "subregion", "capital"]
    for col in cols_texto:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()

    # --- Conversión de tipos ---
    df["poblacion"] = pd.to_numeric(df["poblacion"], errors="coerce").astype("Int64")
    df["area_km2"]  = pd.to_numeric(df["area_km2"],  errors="coerce")

    # --- Columna derivada: densidad poblacional ---
    df["densidad_hab_km2"] = (df["poblacion"] / df["area_km2"]).round(2)

    # --- Ordenar por población descendente ---
    df.sort_values("poblacion", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    logger.info(
        "Transformación completada: %d registros, %d columnas.", len(df), len(df.columns)
    )
    return df


def _extraer_moneda(currencies: dict) -> str | None:
    """Extrae el nombre de la primera moneda del diccionario de monedas."""
    if not currencies:
        return None
    primera = next(iter(currencies.values()), {})
    return primera.get("name")


def _extraer_idiomas(languages: dict) -> str | None:
    """Concatena todos los idiomas en una sola cadena separada por comas."""
    if not languages:
        return None
    return ", ".join(languages.values())


# ─── ETAPA 3: CARGA ──────────────────────────────────────────────────────────
def load(df: pd.DataFrame, filepath: str) -> None:
    """
    Carga el DataFrame transformado en un archivo CSV.

    Args:
        df:       DataFrame limpio y transformado.
        filepath: Ruta del archivo CSV de salida.
    """
    logger.info("Iniciando carga de datos en: %s", filepath)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    logger.info("Carga exitosa: %d registros guardados en '%s'.", len(df), filepath)


# ─── PUNTO DE ENTRADA ────────────────────────────────────────────────────────
def run_pipeline() -> None:
    """Ejecuta el pipeline ETL completo: Extract → Transform → Load."""
    inicio = datetime.now()
    logger.info("=" * 60)
    logger.info("PIPELINE ETL INICIADO")
    logger.info("=" * 60)

    try:
        raw       = extract(API_URL)
        df_clean  = transform(raw)
        load(df_clean, OUTPUT_FILE)

        duracion = (datetime.now() - inicio).total_seconds()
        logger.info("=" * 60)
        logger.info("PIPELINE FINALIZADO EXITOSAMENTE en %.2f segundos", duracion)
        logger.info("Archivo de salida: %s", OUTPUT_FILE)
        logger.info("=" * 60)

        # Vista previa en consola
        print("\n── Vista previa del dataset generado ──")
        print(df_clean[["nombre_comun", "region", "poblacion",
                         "area_km2", "densidad_hab_km2"]].head(10).to_string(index=False))

    except requests.exceptions.RequestException as e:
        logger.error("Error al conectar con la API: %s", e)
        raise
    except Exception as e:
        logger.error("Error inesperado en el pipeline: %s", e)
        raise


if __name__ == "__main__":
    run_pipeline()
