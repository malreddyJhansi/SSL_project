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
    schema = get_cert_schema()
    fetch_ssl_cert_udf = F.udf(lambda h, p: fetch_ssl_cert(h, p, expiry_threshold), schema)
    domain_df = spark.table("ssl_hosts_final")

    cert_df = domain_df.withColumn("cert_details", fetch_ssl_cert_udf("hostname", "port")) \
                       .select("hostname", "port", "cert_details.*")

    cert_df = cert_df.withColumn("run_date", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    cert_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("ssl_hosts_final_results")

    df_all = spark.table("ssl_hosts_final_results")
    latest_run_date = df_all.select("run_date").orderBy(F.col("run_date").desc()).limit(1).collect()[0][0]
    df = df_all.filter(F.col("run_date") == F.lit(latest_run_date))
    
    df = df.withColumn("cert_status", F.lower(F.col("cert_status")))

    expired = [r.asDict() for r in df.filter(F.lower(df.cert_status).contains("expired")).collect()]
    expiring = [r.asDict() for r in df.filter(F.lower(df.cert_status).contains("expiring")).collect()]
    # Remove expired from invalid to avoid duplicates
    invalid = [r.asDict() for r in df.filter(
        (df.issue_category != "SSL_CERT_OK") & (~F.lower(df.cert_status).contains("expired"))
    ).collect()]

    return expired, expiring, invalid
