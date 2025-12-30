import psycopg2
from psycopg2 import sql

# PostgreSQL 연결
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="analytics_db",
    user="admin",
    password="admin123"
)

cursor = conn.cursor()

print("🗄️  데이터베이스 테이블 생성 중...")

# 테이블 1: 인기 상품 통계
cursor.execute("""
    DROP TABLE IF EXISTS popular_products;
    CREATE TABLE popular_products (
        id SERIAL PRIMARY KEY,
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        product_name VARCHAR(100),
        order_count INTEGER,
        total_sales BIGINT,
        created_at TIMESTAMP DEFAULT NOW()
    );
""")

# 테이블 2: 의심 IP
cursor.execute("""
    DROP TABLE IF EXISTS suspicious_ips;
    CREATE TABLE suspicious_ips (
        id SERIAL PRIMARY KEY,
        window_start TIMESTAMP,
        ip_address VARCHAR(50),
        order_count INTEGER,
        total_spent BIGINT,
        created_at TIMESTAMP DEFAULT NOW()
    );
""")

# 테이블 3: 고액 거래
cursor.execute("""
    DROP TABLE IF EXISTS high_value_orders;
    CREATE TABLE high_value_orders (
        id SERIAL PRIMARY KEY,
        order_time TIMESTAMP,
        order_id VARCHAR(100),
        product_name VARCHAR(100),
        quantity INTEGER,
        total_price BIGINT,
        ip_address VARCHAR(50),
        created_at TIMESTAMP DEFAULT NOW()
    );
""")

# 테이블 4: 실시간 통계 (대시보드용)
cursor.execute("""
    DROP TABLE IF EXISTS realtime_stats;
    CREATE TABLE realtime_stats (
        id SERIAL PRIMARY KEY,
        stat_time TIMESTAMP,
        total_orders INTEGER,
        total_sales BIGINT,
        avg_order_value BIGINT,
        suspicious_ip_count INTEGER,
        created_at TIMESTAMP DEFAULT NOW()
    );
""")

conn.commit()

print("✅ 테이블 생성 완료!")
print("\n생성된 테이블:")
print("  1. popular_products - 인기 상품")
print("  2. suspicious_ips - 의심 IP")
print("  3. high_value_orders - 고액 거래")
print("  4. realtime_stats - 실시간 통계")

cursor.close()
conn.close()