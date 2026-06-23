from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round

def main():
    spark = SparkSession.builder \
        .appName("EcommerceTransform") \
        .config("spark.hadoop.fs.s3a.impl", 
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
        .getOrCreate()

    BUCKET = "ecommerce-pipeline-abhay"

    # ── Read raw products from S3 ──────────────────
    df = spark.read.option("multiline", "true") \
               .json(f"s3a://{BUCKET}/raw/products.json")

    # ── Transformations ────────────────────────────
    df_transformed = df \
        .withColumn("price", col("price").cast("double")) \
        .withColumn("rating_score",
            col("rating.rate").cast("double")) \
        .withColumn("rating_count",
            col("rating.count").cast("int")) \
        .withColumn("price_bucket",
            when(col("price") < 20,  "Budget")
            .when(col("price") < 50,  "Mid")
            .when(col("price") < 100, "Premium")
            .otherwise("Luxury")) \
        .withColumn("high_rated",
            when(col("rating_score") >= 4.0, "Yes")
            .otherwise("No")) \
        .withColumn("rating_score",
            round(col("rating_score"), 2)) \
        .drop("rating")

    # ── Write processed Parquet to S3 ─────────────
    df_transformed.write.mode("overwrite") \
        .parquet(f"s3a://{BUCKET}/processed/products/")

    print("✅ Spark transform complete!")
    spark.stop()

if __name__ == "__main__":
    main()