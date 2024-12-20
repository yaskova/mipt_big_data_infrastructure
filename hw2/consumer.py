import sqlite3
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "weather",
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

conn = sqlite3.connect("weather.db")
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city TEXT,
    temperature REAL,
    condition TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

for message in consumer:
    data = message.value
    cursor.execute("""
    INSERT INTO weather (city, temperature, condition) VALUES (?, ?, ?)
    """, (data["city"], data["temperature"], data["condition"]))
    conn.commit()
    print(f"Saved: {data}")