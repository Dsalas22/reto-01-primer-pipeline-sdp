import re

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


SUFFIX = spark.conf.get("pipelines.suffix", "").strip().lower()
if not re.fullmatch(r"[a-z]{3}", SUFFIX):
    raise ValueError("Configura pipelines.suffix con tres letras unicas, por ejemplo dsh.")

BRONZE = f"vuelos_bronze_{SUFFIX}"
SCHEMA = StructType([
    StructField("evento_id", StringType()),
    StructField("aeronave_id", StringType()),
    StructField("pais_origen", StringType()),
    StructField("velocidad_kmh", DoubleType()),
])


@dp.table(name=BRONZE)
def vuelos_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SCHEMA)
        .load(spark.conf.get("lab.source_path"))
    )


@dp.materialized_view(name=f"indicadores_vuelos_{SUFFIX}")
def indicadores_vuelos():
    return spark.read.table(BRONZE).agg(
        F.count("*").alias("eventos"),
        F.countDistinct("aeronave_id").alias("aeronaves"),
        F.max("velocidad_kmh").alias("velocidad_maxima"),
    )
