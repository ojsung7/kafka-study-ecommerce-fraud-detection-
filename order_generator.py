from confluent_kafka import Producer
import json
import time
import random
from datetime import datetime

# Kafka Producer 설정
conf = {
    'bootstrap.servers': 'localhost:9092',
}

producer = Producer(conf)

# 상품 목록
products = [
    {"id": "P001", "name": "노트북", "price": 1500000},
    {"id": "P002", "name": "스마트폰", "price": 800000},
    {"id": "P003", "name": "이어폰", "price": 150000},
    {"id": "P004", "name": "마우스", "price": 30000},
    {"id": "P005", "name": "키보드", "price": 80000},
]

# 정상 사용자 IP 풀
normal_ips = [f"192.168.{random.randint(1,50)}.{random.randint(1,255)}" 
              for _ in range(100)]

# 의심스러운 IP (매크로 봇)
suspicious_ip = "192.168.99.99"

def delivery_report(err, msg):
    """메시지 전송 결과 콜백"""
    if err is not None:
        print(f'❌ 전송 실패: {err}')
    # else:
    #     print(f'✅ 전송 성공: {msg.topic()} [{msg.partition()}]')

def generate_normal_order():
    """정상 주문 생성"""
    product = random.choice(products)
    return {
        "order_id": f"ORD{int(time.time() * 1000)}",
        "timestamp": datetime.now().isoformat(),
        "user_id": f"USER{random.randint(1000, 9999)}",
        "product_id": product["id"],
        "product_name": product["name"],
        "price": product["price"],
        "quantity": random.randint(1, 3),
        "ip_address": random.choice(normal_ips),
        "payment_method": random.choice(["card", "cash", "point"])
    }

def generate_suspicious_order():
    """의심스러운 주문 생성 (같은 IP에서 반복)"""
    product = random.choice(products)
    return {
        "order_id": f"ORD{int(time.time() * 1000)}",
        "timestamp": datetime.now().isoformat(),
        "user_id": "USERBOT",
        "product_id": product["id"],
        "product_name": product["name"],
        "price": product["price"],
        "quantity": random.randint(5, 10),  # 수량도 많음
        "ip_address": suspicious_ip,
        "payment_method": "card"
    }

print("🛒 전자상거래 주문 생성 시작!")
print("=" * 60)

order_count = 0

try:
    while True:
        # 90% 정상 주문, 10% 의심 주문
        if random.random() < 0.9:
            order = generate_normal_order()
            emoji = "✅"
        else:
            order = generate_suspicious_order()
            emoji = "🚨"
        
        # Kafka로 전송
        producer.produce(
            'orders',
            key=order['order_id'],
            value=json.dumps(order).encode('utf-8'),
            callback=delivery_report
        )
        
        # 버퍼에 쌓인 메시지 전송
        producer.poll(0)
        
        total_price = order['price'] * order['quantity']
        print(f"{emoji} 주문 #{order_count}: {order['product_name']} "
              f"x{order['quantity']} = {total_price:,}원 "
              f"(IP: {order['ip_address']})")
        
        order_count += 1
        
        # 랜덤한 간격 (실제처럼)
        time.sleep(random.uniform(0.1, 1.0))
        
except KeyboardInterrupt:
    print(f"\n📊 총 {order_count}개 주문 생성 완료")
    producer.flush()