from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("dezc-5") \
    .getOrCreate()

input_csv = "../data/fhv/fhv_tripdata_2019-10.csv.gz"
input_parquet = "results/fhv_tripdata_2019-10"

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

# Perform query
df.createOrReplaceTempView("table")
spark.sql("""
        SELECT
          COUNT(*)
        FROM
            table
        WHERE
                pickup_datetime >= '2019-10-15T00:00:00'
            AND pickup_datetime < '2019-10-16T00:00:00'
        """).show()
