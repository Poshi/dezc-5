from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("dezc-5") \
    .getOrCreate()

input_csv = "../data/fhv/fhv_tripdata_2019-10.csv.gz"

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
df = spark.read.schema(inferred_schema).csv(input_csv, header=True)

# Repartition
r_df = df.repartition(6)

# Save in parquet format
r_df.write.parquet("results/fhv_tripdata_2019-10")
