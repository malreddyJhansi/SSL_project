from pyspark.sql import SparkSession, functions as F, types as T
from utils.ssl_utils import fetch_ssl_cert

def init_spark():
    return SparkSession.builder.appName("SSL_Monitor").getOrCreate()

def get_cert_schema():
    return T.StructType([
        T.StructField("is_valid", T.BooleanType(), True),
        T.StructField("cert_issuer", T.StringType(), True),
        T.StructField("cert_subject", T.StringType(), True),
        T.StructField("valid_from", T.StringType(), True),
        T.StructField("valid_to", T.StringType(), True),
        T.StructField("days_to_expiry", T.IntegerType(), True),
        T.StructField("cert_status", T.StringType(), True),
        T.StructField("issue_category", T.StringType(), True),
        T.StructField("error_message", T.StringType(), True),
        T.StructField("alert_type", T.StringType(), True)
    ])

def process_domains(spark, expiry_threshold):
    """
    Reads domain list, applies SSL check UDF, writes results to Delta table,
    and returns categorized results for PDF report.
    """

    # Define schema for UDF output
    schema = get_cert_schema()

    # Register the UDF
    fetch_ssl_cert_udf = F.udf(lambda h, p: fetch_ssl_cert(h, p, expiry_threshold), schema)

    # Read input table (hostname + port)
    domain_df = spark.table("ssl_hosts_final")

    # Apply SSL check
    cert_df = domain_df.withColumn("cert_details", fetch_ssl_cert_udf("hostname", "port")) \
                       .select(
                           "hostname",
                           "port",
                           "cert_details.*"
                       )

    # Add run timestamp
    cert_df = cert_df.withColumn("run_date", F.current_timestamp())

    # Write results to output Delta table
    cert_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("ssl_hosts_final_results")

    # Read back only the latest run
    df_all = spark.table("ssl_hosts_final_results")
    latest_run_date = df_all.select("run_date").orderBy(F.col("run_date").desc()).limit(1).collect()[0][0]
    df = df_all.filter(F.col("run_date") == F.lit(latest_run_date))

    # --- 🔧 Normalize status and category to lowercase ---
    df = df.withColumn("cert_status", F.lower(F.col("cert_status")))
    df = df.withColumn("issue_category", F.lower(F.col("issue_category")))

    # --- 🧩 Classify into groups (no duplicates) ---
    expired = [r.asDict() for r in df.filter(F.col("cert_status").contains("expired")).collect()]
    expiring = [r.asDict() for r in df.filter(F.col("cert_status").contains("expiring")).collect()]
    # Invalid = all SSL errors that are NOT expired or expiring
    invalid = [r.asDict() for r in df.filter(
        (F.col("issue_category") != "ssl_cert_ok") &
        (~F.col("cert_status").contains("expired")) &
        (~F.col("cert_status").contains("expiring"))
    ).collect()]

    # --- 🧾 Optional: Log summary ---
    total_count = df.count()
    logger.info(f"✅ Total checked: {total_count}")
    logger.info(f"📍 Expired: {len(expired)} | Expiring Soon: {len(expiring)} | Invalid: {len(invalid)}")

    return expired, expiring, invalid
