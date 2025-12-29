from confluent_kafka import Consumer, KafkaError
import json
from datetime import datetime, timedelta
from collections import defaultdict

# Kafka Consumer 설정
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fraud-detector-group',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
consumer.subscribe(['orders'])

# IP별 주문 기록 (최근 10분)
ip_orders = defaultdict(list)

# 고액 결제 임계값
HIGH_VALUE_THRESHOLD = 500000

print("🔍 실시간 이상 거래 탐지 시작!")
print("=" * 60)

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"❌ Consumer 오류: {msg.error()}")
                break
        
        # 메시지 파싱
        order = json.loads(msg.value().decode('utf-8'))
        
        ip = order['ip_address']
        timestamp = datetime.fromisoformat(order['timestamp'])
        total_price = order['price'] * order['quantity']
        
        # 현재 시간 기준 10분 이내 주문만 유지
        ten_minutes_ago = datetime.now() - timedelta(minutes=10)
        ip_orders[ip] = [
            (ts, price) for ts, price in ip_orders[ip] 
            if ts > ten_minutes_ago
        ]
        
        # 현재 주문 추가
        ip_orders[ip].append((timestamp, total_price))
        
        # 이상 탐지 로직
        recent_orders = len(ip_orders[ip])
        
        # 🚨 의심 패턴 1: 같은 IP에서 10분 내 10번 이상 주문
        if recent_orders >= 10:
            print(f"\n{'='*60}")
            print(f"🚨🚨🚨 매크로 의심! IP: {ip}")
            print(f"   └─ 10분 내 {recent_orders}번 주문")
            print(f"   └─ 주문 ID: {order['order_id']}")
            print(f"{'='*60}\n")
        
        # 💰 의심 패턴 2: 고액 결제
        elif total_price >= HIGH_VALUE_THRESHOLD:
            print(f"\n💰 고액 결제 발생!")
            print(f"   └─ 금액: {total_price:,}원")
            print(f"   └─ 상품: {order['product_name']} x{order['quantity']}")
            print(f"   └─ IP: {ip}\n")
        
        # 정상 주문
        else:
            print(f"✅ 정상: {order['product_name']} "
                  f"{total_price:,}원 (IP 최근 주문: {recent_orders}건)")

except KeyboardInterrupt:
    print("\n🛑 이상 탐지 종료")

finally:
    consumer.close()