# consumer_to_clickhouse.py
from kafka import KafkaConsumer
import json
import clickhouse_connect


consumer = KafkaConsumer(
    "events_user",
    bootstrap_servers="localhost:9092",
    group_id ="clickhouse_consumer_group",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

client = clickhouse_connect.get_client(host='localhost', port=8123, username='user', password='strongpassword')

client.command("""
CREATE TABLE IF NOT EXISTS user_logins (
    username String,
    event_type String,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY event_time
""")

for message in consumer:
    try:
        data = message.value
        print("Received:", data)
        client.command(
            f"INSERT INTO user_logins (username, event_type, event_time) VALUES ('{data['user']}', '{data['event']}', toDateTime({data['timestamp']}))"
        )
        consumer.commit()
    except Exception as e:
        print(f"Error processing message: {e}")