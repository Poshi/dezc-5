from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("dezc-5") \
    .getOrCreate()

input_csv = "../data/fhv/fhv_tripdata_2019-10.csv.gz"
input_parquet = "results/fhv_tripdata_2019-10"
taxi_zones = "../data/taxi_zone_lookup.csv"

if not Path(input_parquet).exists():
    # Infer schema
    inferred_schema = (
        spark
            .read
            .option("inferSchema", True)
            .option("header", True)
            .csv(
                spark
                    .read
                    .text(input_csv)
                    .limit(100)
                    .rdd
                    .flatMap(lambda x: x)
            )
            .schema
    )

    # Load data
    df = spark.read.schema(inferred_schema).csv(input_csv, header=True).repartition(6)
else:
    # Just load existing data
    df = spark.read.parquet(input_parquet)

zones_df = spark.read.option("header", True).csv(taxi_zones)

# Perform query
df.createOrReplaceTempView("table")
zones_df.createOrReplaceTempView("zones")

spark.sql("""
        SELECT
            z.Zone
        FROM
            table t JOIN zones z ON t.PULocationID = z.LocationID
        GROUP BY
            z.Zone
        ORDER BY
            COUNT(*)
        LIMIT 1
        """).show()
