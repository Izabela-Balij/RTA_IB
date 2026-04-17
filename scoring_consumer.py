from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

def score_transaction(tx):
    score = 0
    rules = []
    if tx.get('amount', 0) > 3000:
        score += 3
        rules.append('R1: kwota > 3000')
    if tx.get('category') == 'elektronika' and tx.get('amount', 0) > 1500:
        score += 2
        rules.append('R2: elektronika + kwota > 1500')
    hour = tx.get('hour')
    if hour is None:
        try:
            hour = datetime.fromisoformat(tx['timestamp']).hour
        except Exception:
            hour = 12
    if hour < 6:
        score += 2
        rules.append('R3: godzina nocna (< 6)')
    return score, rules

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

THRESHOLD = 3
print(f"=== Scoring consumer — próg alertu: score >= {THRESHOLD} ===")
for message in consumer:
    tx = message.value
    score, rules = score_transaction(tx)
    tx['score'] = score
    tx['rules_triggered'] = rules
    if score >= THRESHOLD:
        alert_producer.send('alerts', value=tx)
        alert_producer.flush()
        print(f"ALERT | {tx['tx_id']} | {tx['amount']:8.2f} PLN | score={score} | {rules}")
    else:
        print(f"OK    | {tx['tx_id']} | {tx['amount']:8.2f} PLN | score={score}")
