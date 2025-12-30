from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("🚀 Spark 초기화 중...")

spark = SparkSession.builder \
    .appName("EcommerceToDatabase") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0") \
    .config("spark.sql.session.timeZone", "Asia/Seoul") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark 준비 완료! (타임존: Asia/Seoul)")

# PostgreSQL 연결 정보
jdbc_url = "jdbc:postgresql://localhost:5432/analytics_db"
db_properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# Kafka 스트림 읽기
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

schema = StructType([
    StructField("order_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("user_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("price", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("ip_address", StringType()),
    StructField("payment_method", StringType())
])

orders = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
 .withColumn("timestamp", to_timestamp(col("timestamp"))) \
 .withColumn("total_price", col("price") * col("quantity"))

print("\n" + "=" * 80)
print("💾 PostgreSQL 저장 시작! (KST 시간)")
print("=" * 80)

# ==========================================
# 1. 인기 상품 → DB 저장
# ==========================================
popular_products = orders \
    .withWatermark("timestamp", "30 seconds") \
    .groupBy(
        window(col("timestamp"), "30 seconds"),
        col("product_name")
    ) \
    .agg(
        count("*").alias("order_count"),
        sum("total_price").alias("total_sales")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("product_name"),
        col("order_count"),
        col("total_sales")
    )

def save_popular_products(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write \
            .jdbc(url=jdbc_url, table="popular_products", 
                  mode="append", properties=db_properties)
        print(f"✅ [Batch {batch_id}] 인기 상품 {batch_df.count()}건 저장")

query1 = popular_products \
    .writeStream \
    .foreachBatch(save_popular_products) \
    .outputMode("update") \
    .trigger(processingTime='30 seconds') \
    .start()

# ==========================================
# 2. 의심 IP → DB 저장
# ==========================================
suspicious_ips = orders \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("ip_address")
    ) \
    .agg(
        count("*").alias("order_count"),
        sum("total_price").alias("total_spent")
    ) \
    .filter(col("order_count") >= 10) \
    .select(
        col("window.start").alias("window_start"),
        col("ip_address"),
        col("order_count"),
        col("total_spent")
    )

def save_suspicious_ips(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write \
            .jdbc(url=jdbc_url, table="suspicious_ips", 
                  mode="append", properties=db_properties)
        print(f"🚨 [Batch {batch_id}] 의심 IP {batch_df.count()}건 저장")

query2 = suspicious_ips \
    .writeStream \
    .foreachBatch(save_suspicious_ips) \
    .outputMode("update") \
    .trigger(processingTime='30 seconds') \
    .start()

# ==========================================
# 3. 고액 거래 → DB 저장
# ==========================================
high_value = orders \
    .filter(col("total_price") >= 500000) \
    .select(
        col("timestamp").alias("order_time"),
        col("order_id"),
        col("product_name"),
        col("quantity"),
        col("total_price"),
        col("ip_address")
    )

def save_high_value(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write \
            .jdbc(url=jdbc_url, table="high_value_orders", 
                  mode="append", properties=db_properties)
        print(f"💰 [Batch {batch_id}] 고액 거래 {batch_df.count()}건 저장")

query3 = high_value \
    .writeStream \
    .foreachBatch(save_high_value) \
    .outputMode("append") \
    .start()

# ==========================================
# 4. 실시간 통계 → DB 저장
# ==========================================
stats = orders \
    .withWatermark("timestamp", "30 seconds") \
    .groupBy(window(col("timestamp"), "30 seconds")) \
    .agg(
        count("*").alias("total_orders"),
        sum("total_price").alias("total_sales"),
        avg("total_price").alias("avg_order_value")
    ) \
    .select(
        col("window.end").alias("stat_time"),
        col("total_orders"),
        col("total_sales"),
        col("avg_order_value").cast("bigint")
    ) \
    .withColumn("suspicious_ip_count", lit(0))

def save_stats(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write \
            .jdbc(url=jdbc_url, table="realtime_stats", 
                  mode="append", properties=db_properties)
        print(f"📊 [Batch {batch_id}] 실시간 통계 저장")

query4 = stats \
    .writeStream \
    .foreachBatch(save_stats) \
    .outputMode("update") \
    .trigger(processingTime='30 seconds') \
    .start()

print("\n📊 저장 대시보드:")
print("  [1] 인기 상품 - 30초마다 (KST)")
print("  [2] 의심 IP - 30초마다 (KST)")
print("  [3] 고액 거래 - 실시간 (KST)")
print("  [4] 통계 - 30초마다 (KST)")
print("\n💡 Ctrl+C를 눌러 종료하세요\n")

try:
    query1.awaitTermination()
except KeyboardInterrupt:
    print("\n🛑 저장 중단")
    spark.stop()