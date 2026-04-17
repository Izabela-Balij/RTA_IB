from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'alerts',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='alerts-reader-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
)

print("=== Konsument alertów — nasłuchuje na temacie 'alerts' ===")

for message in consumer:
    tx = message.value
    print(
        f"[{message.offset:>5}] "
        f"{tx['tx_id']} | {tx['amount']:8.2f} PLN | "
        f"{tx.get('store','?'):<10} | {tx.get('category','?'):<12} | "
        f"score={tx.get('score','?')} | "
        f"reguły={tx.get('rules_triggered', [])}"
    )
