from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("🚀 Spark 초기화 중...")

# Spark Session 생성
spark = SparkSession.builder \
    .appName("EcommerceRealTimeAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark 준비 완료!")

# Kafka에서 데이터 스트림 읽기
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

# JSON 스키마 정의
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

# JSON 파싱 + timestamp를 실제 timestamp 타입으로 변환
orders = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
 .withColumn("timestamp", to_timestamp(col("timestamp")))

# 총 금액 계산
orders_with_total = orders.withColumn(
    "total_price", 
    col("price") * col("quantity")
)

print("\n" + "=" * 80)
print("🔥 Spark Streaming 실시간 분석 시작!")
print("=" * 80)

# ==========================================
# 분석 1: 최근 30초간 인기 상품 Top 5
# ==========================================
popular_products = orders_with_total \
    .withWatermark("timestamp", "30 seconds") \
    .groupBy(
        window(col("timestamp"), "30 seconds", "10 seconds"),
        col("product_name")
    ) \
    .agg(
        count("*").alias("주문수"),
        sum("total_price").alias("총매출")
    ) \
    .select(
        col("window.start").alias("시작시간"),
        col("window.end").alias("종료시간"),
        col("product_name").alias("상품명"),
        col("주문수"),
        format_number(col("총매출"), 0).alias("총매출")
    ) \
    .orderBy(col("주문수").desc())

# ==========================================
# 분석 2: 의심 IP (최근 1분간 10건 이상)
# ==========================================
suspicious_ips = orders_with_total \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("ip_address")
    ) \
    .agg(
        count("*").alias("주문수"),
        sum("total_price").alias("총금액"),
        collect_list("product_name").alias("구매상품")
    ) \
    .filter(col("주문수") >= 10) \
    .select(
        col("window.start").alias("시작시간"),
        col("ip_address").alias("IP주소"),
        col("주문수"),
        format_number(col("총금액"), 0).alias("총금액"),
        col("구매상품")
    )

# ==========================================
# 분석 3: 고액 거래 (50만원 이상)
# ==========================================
high_value = orders_with_total \
    .filter(col("total_price") >= 500000) \
    .select(
        col("timestamp").alias("시간"),
        col("order_id").alias("주문ID"),
        col("product_name").alias("상품명"),
        col("quantity").alias("수량"),
        format_number(col("total_price"), 0).alias("금액"),
        col("ip_address").alias("IP")
    )

# ==========================================
# 콘솔 출력 함수 정의
# ==========================================

def print_header(batch_df, batch_id, title, emoji):
    """배치마다 헤더 출력"""
    print("\n" + "=" * 80)
    print(f"{emoji} {title} (Batch #{batch_id})")
    print("=" * 80)

# ==========================================
# 쿼리 1: 인기 상품 Top 5 (10초마다)
# ==========================================
print("\n📊 [분석 1] 인기 상품 순위 - 매 10초마다 업데이트")
print("   └─ 최근 30초간 가장 많이 팔린 상품을 추적합니다\n")

query1 = popular_products \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, id: (
        print_header(df, id, "인기 상품 Top 5", "🏆"),
        df.show(5, truncate=False)
    )) \
    .trigger(processingTime='10 seconds') \
    .start()

# ==========================================
# 쿼리 2: 의심 IP 탐지 (30초마다)
# ==========================================
print("\n🚨 [분석 2] 이상 거래 탐지 - 매 30초마다 체크")
print("   └─ 같은 IP에서 1분 내 10건 이상 주문한 경우 알림\n")

query2 = suspicious_ips \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, id: (
        print_header(df, id, "⚠️  매크로 의심 IP 발견!", "🚨") if df.count() > 0 else None,
        df.show(truncate=False) if df.count() > 0 else print("   ✅ 의심스러운 IP 없음")
    )) \
    .trigger(processingTime='30 seconds') \
    .start()

# ==========================================
# 쿼리 3: 고액 거래 실시간 알림
# ==========================================
print("\n💰 [분석 3] 고액 거래 모니터링 - 실시간")
print("   └─ 50만원 이상 거래 발생 시 즉시 알림\n")

def show_high_value(batch_df, batch_id):
    if batch_df.count() > 0:
        print("\n" + "=" * 80)
        print(f"💳 💰 고액 거래 발생! (Batch #{batch_id})")
        print("=" * 80)
        batch_df.show(truncate=False)
        print(f"총 {batch_df.count()}건의 고액 거래")
    # count가 0이면 아무것도 출력 안 함

query3 = high_value \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(show_high_value) \
    .start()

print("=" * 80)
print("✅ 모든 분석 쿼리 실행 중...")
print("=" * 80)
print("\n💡 Ctrl+C를 눌러 종료하세요\n")

# 스트리밍 유지
try:
    query1.awaitTermination()
except KeyboardInterrupt:
    print("\n\n" + "=" * 80)
    print("🛑 Spark Streaming 종료")
    print("=" * 80)
    spark.stop()