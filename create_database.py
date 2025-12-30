import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

print("🗄️  데이터베이스 생성 중...")

# 먼저 기본 postgres DB에 연결
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="postgres",  # 기본 DB
    user="admin",
    password="admin123"
)

# AUTOCOMMIT 모드로 변경 (데이터베이스 생성을 위해 필요)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cursor = conn.cursor()

# analytics_db가 이미 있는지 확인
cursor.execute("SELECT 1 FROM pg_database WHERE datname='analytics_db'")
exists = cursor.fetchone()

if not exists:
    cursor.execute("CREATE DATABASE analytics_db")
    print("✅ analytics_db 생성 완료!")
else:
    print("✅ analytics_db가 이미 존재합니다.")

cursor.close()
conn.close()

print("\n이제 create_tables.py를 실행하세요!")