import sqlite3
import json

conn = sqlite3.connect("weather.db")
cursor = conn.cursor()

cursor.execute("SELECT * FROM weather")
rows = cursor.fetchall()

data = []
for row in rows:
    data.append({
        "id": row[0],
        "city": row[1],
        "temperature": row[2],
        "condition": row[3],
        "timestamp": row[4]
    })

with open("weather_data.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4)

print("Data exported to weather_data.json")