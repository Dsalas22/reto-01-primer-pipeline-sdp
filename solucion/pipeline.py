# Reto 01: Bronce (raw_data.bronze) -> Plata Data Vault (data_vault.silver) -> Oro (ods.gold)
import re

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


# ---------- Configuracion (Settings > Configuration) ----------
# Obligatorio: pipelines.suffix (tres letras). Opcionales, con estos valores por defecto:
# raw_catalog=raw_data, raw_schema=bronze, silver_catalog=data_vault, silver_schema=silver,
# gold_catalog=ods, gold_schema=gold, landing_volume=landing_retos_sdp
SUFFIX = spark.conf.get("pipelines.suffix", "").strip().lower()
if not re.fullmatch(r"[a-z]{3}", SUFFIX):
    raise ValueError("Configura pipelines.suffix con tres letras unicas, por ejemplo dsh.")


def config(clave, defecto):
    valor = spark.conf.get(clave, defecto).strip()
    if not re.fullmatch(r"[A-Za-z0-9_]+", valor):
        raise ValueError(f"Valor invalido para {clave}: '{valor}'. Usa solo letras, numeros y guion bajo.")
    return valor


RAW_CATALOG = config("raw_catalog", "raw_data")
RAW_SCHEMA = config("raw_schema", "bronze")
PLATA = f"{config('silver_catalog', 'data_vault')}.{config('silver_schema', 'silver')}"
ORO = f"{config('gold_catalog', 'ods')}.{config('gold_schema', 'gold')}"
VOLUMEN = config("landing_volume", "landing_retos_sdp")

# Cada participante aterriza sus archivos en su propia carpeta del volumen compartido.
RUTA_ENTRADA = f"/Volumes/{RAW_CATALOG}/{RAW_SCHEMA}/{VOLUMEN}/{SUFFIX}/reto01/entrada"

# Nombres con prefijo del reto y sufijo del participante.
BRONCE = f"{RAW_CATALOG}.{RAW_SCHEMA}.r01_vuelos_raw_{SUFFIX}"
STAGING = f"r01_stg_vuelos_{SUFFIX}"
HUB_VUELO = f"{PLATA}.r01_hub_vuelo_{SUFFIX}"
SAT_VUELO = f"{PLATA}.r01_sat_vuelo_{SUFFIX}"
INDICADORES = f"{ORO}.r01_indicadores_vuelos_{SUFFIX}"

SCHEMA = StructType([
    StructField("evento_id", StringType()),
    StructField("aeronave_id", StringType()),
    StructField("pais_origen", StringType()),
    StructField("velocidad_kmh", DoubleType()),
    StructField("persona_id", StringType()),
    StructField("persona_alias", StringType()),
])

# Atributos descriptivos del vuelo que van al satelite.
ATRIBUTOS = ["aeronave_id", "pais_origen", "velocidad_kmh", "persona_id", "persona_alias"]


def normalizar(columna):
    """Texto en mayusculas, sin espacios y sin nulos: base estable para calcular hashes."""
    return F.upper(F.trim(F.coalesce(F.col(columna).cast("string"), F.lit(""))))


# ---------- Bronce: el dato tal como llega, mas columnas tecnicas ----------
@dp.table(name=BRONCE, comment="Bronce: eventos JSON tal como llegan, con archivo de origen y fecha de ingesta.")
def r01_vuelos_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SCHEMA)
        .load(RUTA_ENTRADA)
        .select(
            "*",
            F.col("_metadata.file_path").alias("archivo_origen"),
            F.col("_metadata.file_modification_time").alias("fecha_archivo"),
            F.current_timestamp().alias("fecha_ingesta"),
        )
    )


# ---------- Staging privado: llaves hash y metadatos Data Vault ----------
@dp.temporary_view(name=STAGING, comment="Staging: hash key, hashdiff, fecha_carga y fuente_registro.")
def r01_stg_vuelos():
    return spark.readStream.table(BRONCE).select(
        F.sha2(normalizar("evento_id"), 256).alias("hk_vuelo"),
        "evento_id",
        F.sha2(F.concat_ws("||", *[normalizar(c) for c in ATRIBUTOS]), 256).alias("hashdiff"),
        *ATRIBUTOS,
        F.col("fecha_archivo").alias("fecha_carga"),
        F.col("archivo_origen").alias("fuente_registro"),
    )


# ---------- Plata (Data Vault) ----------
# Los eventos son inmutables: cada evento llega una sola vez, por eso basta con agregar filas.
@dp.table(name=HUB_VUELO, comment="Hub: una fila por llave de negocio evento_id.")
def r01_hub_vuelo():
    return spark.readStream.table(STAGING).select("hk_vuelo", "evento_id", "fecha_carga", "fuente_registro")


@dp.table(name=SAT_VUELO, comment="Satelite: atributos descriptivos del vuelo con su hashdiff.")
def r01_sat_vuelo():
    return spark.readStream.table(STAGING).select("hk_vuelo", "hashdiff", *ATRIBUTOS, "fecha_carga", "fuente_registro")


# ---------- Oro: indicadores (vista materializada, lectura batch) ----------
@dp.materialized_view(name=INDICADORES, comment="Oro: total de eventos, aeronaves distintas y velocidad maxima.")
def r01_indicadores_vuelos():
    return spark.read.table(SAT_VUELO).agg(
        F.count("*").alias("eventos"),
        F.countDistinct("aeronave_id").alias("aeronaves"),
        F.max("velocidad_kmh").alias("velocidad_maxima"),
    )
