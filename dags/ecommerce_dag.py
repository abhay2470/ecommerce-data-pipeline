from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import boto3
import psycopg2
import pandas as pd
import re
import random

# ── Config ─────────────────────────────────────────
BUCKET = "ecommerce-pipeline-abhay"

default_args = {
    "owner": "abhay",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Task 1: Fetch API Data ─────────────────────────
def fetch_api_data():
    print("📦 Fetching data from Fake Store API...")
    products = requests.get("https://fakestoreapi.com/products").json()
    carts    = requests.get("https://fakestoreapi.com/carts").json()
    users    = requests.get("https://fakestoreapi.com/users").json()
    with open("/tmp/products.json", "w") as f:
        json.dump(products, f)
    with open("/tmp/carts.json", "w") as f:
        json.dump(carts, f)
    with open("/tmp/users.json", "w") as f:
        json.dump(users, f)
    print(f"✅ Fetched {len(products)} products, {len(carts)} carts, {len(users)} users")

# ── Task 2: Upload Raw to S3 ───────────────────────
def upload_raw_to_s3():
    print("☁️  Uploading raw data to S3...")
    s3 = boto3.client("s3", region_name="us-east-1")
    for filename in ["products.json", "carts.json", "users.json"]:
        s3.upload_file(f"/tmp/{filename}", BUCKET, f"raw/{filename}")
        print(f"✅ Uploaded {filename} → s3://{BUCKET}/raw/")

# ── Task 3: Validate Raw Data ──────────────────────
def validate_raw_data():
    print("🔍 Validating raw data...")
    with open("/tmp/products.json") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    null_count = df.isnull().sum().sum()
    print(f"📊 Total records : {len(df)}")
    print(f"📊 Null values   : {null_count}")
    assert len(df) > 0, "❌ No data fetched!"
    assert null_count < len(df) * 0.1, "❌ Too many nulls!"
    print("✅ Validation passed!")

# ── Task 4: Clean & Enrich Data ───────────────────
def clean_and_enrich_data():
    print("🐍 Python: Cleaning and enriching data...")
    with open("/tmp/products.json") as f:
        products = json.load(f)
    df = pd.DataFrame(products)
    df["title_clean"]  = df["title"].apply(
        lambda x: re.sub(r"[^a-zA-Z0-9\s]", "", x).strip())
    df["rating_score"] = df["rating"].apply(lambda x: x["rate"])
    df["rating_count"] = df["rating"].apply(lambda x: x["count"])
    df.drop(columns=["rating"], inplace=True)

    def get_price_bucket(price):
        if price < 20:    return "Budget"
        elif price < 50:  return "Mid"
        elif price < 100: return "Premium"
        else:             return "Luxury"

    df["price_bucket"] = df["price"].apply(get_price_bucket)
    df["discount_suggestion"] = df.apply(
        lambda row: "10% OFF" if row["rating_score"] >= 4.5
        else "5% OFF"  if row["rating_score"] >= 4.0
        else "No Discount", axis=1)
    random.seed(42)
    df["stock_units"]  = [random.randint(1, 200) for _ in range(len(df))]
    df["stock_status"] = df["stock_units"].apply(
        lambda x: "Low Stock" if x < 20 else "In Stock")
    df.to_json("/tmp/products_clean.json", orient="records")
    df.to_csv("/tmp/products_clean.csv", index=False)
    print(f"✅ Cleaned {len(df)} products")

# ── Task 5: Calculate KPIs ─────────────────────────
def calculate_kpis():
    print("🐍 Python: Calculating KPIs...")
    df_products = pd.read_json("/tmp/products_clean.json")
    with open("/tmp/carts.json") as f:
        carts = json.load(f)
    orders = []
    for cart in carts:
        for item in cart["products"]:
            product = df_products[df_products["id"] == item["productId"]]
            if not product.empty:
                price = float(product["price"].values[0])
                orders.append({
                    "cart_id":      cart["id"],
                    "user_id":      cart["userId"],
                    "product_id":   item["productId"],
                    "category":     product["category"].values[0],
                    "quantity":     item["quantity"],
                    "unit_price":   price,
                    "total_amount": price * item["quantity"],
                    "order_date":   cart["date"][:10]
                })
    df_orders = pd.DataFrame(orders)
    kpis = {
        "total_orders":     int(len(df_orders)),
        "total_revenue":    round(float(df_orders["total_amount"].sum()), 2),
        "avg_order_value":  round(float(df_orders["total_amount"].mean()), 2),
        "top_category":     df_orders.groupby("category")["total_amount"]
                                     .sum().idxmax(),
        "total_units_sold": int(df_orders["quantity"].sum()),
    }
    print("📊 KPIs:")
    for k, v in kpis.items():
        print(f"   {k}: {v}")
    with open("/tmp/kpis.json", "w") as f:
        json.dump(kpis, f, indent=2)
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.upload_file("/tmp/kpis.json", BUCKET, "processed/kpis/kpis.json")
    df_orders.to_csv("/tmp/orders_clean.csv", index=False)
    print("✅ KPIs saved!")

# ── Task 6: PySpark Transform ──────────────────────
# ── Task 6: PySpark Transform ──────────────────────
def run_spark_transform():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, round as spark_round
    import json
    import boto3

    print("⚡ PySpark: Starting transformation...")

    # Simple local Spark session — no S3 config needed
    spark = SparkSession.builder \
        .appName("EcommerceTransform") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Read from local /tmp
    df = spark.read.option("multiline", "true") \
               .json("/tmp/products_clean.json")

    # Transformations
    df_transformed = df \
        .withColumn("price", col("price").cast("double")) \
        .withColumn("price_bucket",
            when(col("price") < 20,  "Budget")
            .when(col("price") < 50,  "Mid")
            .when(col("price") < 100, "Premium")
            .otherwise("Luxury")) \
        .withColumn("high_rated",
            when(col("rating_score") >= 4.0, "Yes")
            .otherwise("No")) \
        .withColumn("rating_score",
            spark_round(col("rating_score").cast("double"), 2))

    # Save as Parquet locally first
    df_transformed.write.mode("overwrite") \
        .parquet("/tmp/products_processed/")

    count = df_transformed.count()
    print(f"✅ Spark transform complete! {count} records processed")
    spark.stop()

    # Now upload parquet files to S3 using boto3
    print("☁️  Uploading Parquet to S3...")
    import os
    s3 = boto3.client("s3", region_name="us-east-1")
    for filename in os.listdir("/tmp/products_processed/"):
        if filename.endswith(".parquet"):
            s3.upload_file(
                f"/tmp/products_processed/{filename}",
                "ecommerce-pipeline-abhay",
                f"processed/products/{filename}"
            )
            print(f"✅ Uploaded {filename} to S3")

# ── Task 7: Load into PostgreSQL ───────────────────
def load_to_postgres():
    print("🗄️ Loading into PostgreSQL...")
    conn = psycopg2.connect(
        host="postgres-db", port=5432,
        database="ecommerce_db",
        user="pipeline_user", password="pipeline_pass"
    )
    cursor = conn.cursor()

    with open("/tmp/products_clean.json") as f:
        products = json.load(f)
    for p in products:
        price = float(p["price"])
        cursor.execute("""
            INSERT INTO dim_products
                (product_id, title, category, price, price_bucket,
                 rating_score, rating_count, high_rated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE SET
                price        = EXCLUDED.price,
                rating_score = EXCLUDED.rating_score
        """, (
            p["id"], p["title_clean"], p["category"], price,
            p["price_bucket"], p["rating_score"], p["rating_count"],
            "Yes" if float(p["rating_score"]) >= 4.0 else "No"
        ))

    with open("/tmp/users.json") as f:
        users = json.load(f)
    for u in users:
        cursor.execute("""
            INSERT INTO dim_users
                (user_id, username, email, city, state)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING
        """, (
            u["id"], u["username"], u["email"],
            u["address"]["city"], "N/A"
        ))

    df_orders = pd.read_csv("/tmp/orders_clean.csv")
    for _, row in df_orders.iterrows():
        cursor.execute("""
            INSERT INTO fact_orders
                (cart_id, user_id, product_id, quantity,
                 unit_price, total_amount, order_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            int(row["cart_id"]), int(row["user_id"]),
            int(row["product_id"]), int(row["quantity"]),
            float(row["unit_price"]), float(row["total_amount"]),
            row["order_date"]
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Data loaded into PostgreSQL!")

# ── Task 8: Run Aggregations ───────────────────────
def run_aggregations():
    print("📊 Running aggregations...")
    conn = psycopg2.connect(
        host="postgres-db", port=5432,
        database="ecommerce_db",
        user="pipeline_user", password="pipeline_pass"
    )
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO agg_category_sales
            (category, total_orders, total_revenue, avg_order_value, report_date)
        SELECT
            p.category,
            COUNT(o.order_id),
            SUM(o.total_amount),
            ROUND(AVG(o.total_amount), 2),
            CURRENT_DATE
        FROM fact_orders o
        JOIN dim_products p ON o.product_id = p.product_id
        GROUP BY p.category
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Aggregations complete!")

# ── Task 9: Generate Daily Report ─────────────────
def generate_daily_report():
    print("🐍 Python: Generating daily report...")
    df_products = pd.read_json("/tmp/products_clean.json")
    df_orders   = pd.read_csv("/tmp/orders_clean.csv")

    category_summary = df_orders.groupby("category").agg(
        total_orders    = ("cart_id",      "count"),
        total_revenue   = ("total_amount", "sum"),
        avg_order_value = ("total_amount", "mean"),
        units_sold      = ("quantity",     "sum")
    ).round(2).reset_index()

    report = {
        "report_date":      datetime.now().strftime("%Y-%m-%d"),
        "generated_at":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_revenue":    round(float(df_orders["total_amount"].sum()), 2),
        "total_orders":     len(df_orders),
        "category_summary": category_summary.to_dict(orient="records"),
        "low_stock_products": df_products[
            df_products["stock_status"] == "Low Stock"
        ][["title_clean", "stock_units"]].to_dict(orient="records")
    }

    report_key = f"reports/{datetime.now().strftime('%Y/%m/%d')}/daily_report.json"
    with open("/tmp/daily_report.json", "w") as f:
        json.dump(report, f, indent=2)

    boto3.client("s3", region_name="us-east-1").upload_file(
        "/tmp/daily_report.json", BUCKET, report_key
    )
    print(f"✅ Report saved → s3://{BUCKET}/{report_key}")
    print(f"   Total Revenue : ${report['total_revenue']}")
    print(f"   Total Orders  : {report['total_orders']}")

# ── DAG Definition ─────────────────────────────────
with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="E-Commerce ETL Pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "etl", "pyspark"]
) as dag:

    task_fetch     = PythonOperator(task_id="fetch_api_data",         python_callable=fetch_api_data)
    task_upload    = PythonOperator(task_id="upload_raw_to_s3",       python_callable=upload_raw_to_s3)
    task_validate  = PythonOperator(task_id="validate_raw_data",      python_callable=validate_raw_data)
    task_clean     = PythonOperator(task_id="clean_and_enrich_data",  python_callable=clean_and_enrich_data)
    task_kpis      = PythonOperator(task_id="calculate_kpis",         python_callable=calculate_kpis)
    task_spark     = PythonOperator(task_id="run_spark_transform",    python_callable=run_spark_transform)
    task_postgres  = PythonOperator(task_id="load_to_postgres",       python_callable=load_to_postgres)
    task_aggregate = PythonOperator(task_id="run_aggregations",       python_callable=run_aggregations)
    task_report    = PythonOperator(task_id="generate_daily_report",  python_callable=generate_daily_report)

    # ── Pipeline Order ─────────────────────────────
    task_fetch >> task_upload >> task_validate >> task_clean >> task_kpis >> task_spark >> task_postgres >> task_aggregate >> task_report