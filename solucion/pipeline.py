from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


SCHEMA = StructType([
    StructField("evento_id", StringType()),
    StructField("aeronave_id", StringType()),
    StructField("pais_origen", StringType()),
    StructField("velocidad_kmh", DoubleType()),
])


@dp.table()
def vuelos_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SCHEMA)
        .load(spark.conf.get("lab.source_path"))
    )


@dp.materialized_view()
def indicadores_vuelos():
    return spark.read.table("vuelos_bronze").agg(
        F.count("*").alias("eventos"),
        F.countDistinct("aeronave_id").alias("aeronaves"),
        F.max("velocidad_kmh").alias("velocidad_maxima"),
    )
