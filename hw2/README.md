# Проект: Система обработки данных о погоде с использованием Apache Kafka

## 1. Скачивание и настройка Kafka

### Шаг 1: Скачивание Kafka
Перейдите на официальный сайт Apache Kafka:
[https://kafka.apache.org/downloads](https://kafka.apache.org/downloads)

1. Скачайте архив Kafka (binary distribution).
2. Распакуйте архив Kafka в удобное место, например, `C:\kafka`.

### Шаг 2: Запуск Zookeeper и Kafka

#### Запуск Zookeeper
1. Откройте командную строку.
2. Перейдите в папку `C:\kafka\bin\windows`.
3. Выполните команду:
   ```
   .\zookeeper-server-start.bat ..\..\config\zookeeper.properties
   ```

#### Запуск Kafka-сервера
1. В новом окне командной строки перейдите в ту же папку `C:\kafka\bin\windows`.
2. Выполните команду:
   ```
   .\kafka-server-start.bat ..\..\config\server.properties
   ```

### Шаг 3: Создание топика
1. В той же папке выполните команду для создания топика `weather` с 3 разделами:
   ```
   .\kafka-topics.bat --create --topic weather --bootstrap-server localhost:9092 --partitions 3
   ```

## 2. Разработка приложения

### Шаг 1: Подготовка проекта

1. Создайте папку для проекта, например, `C:\weather-kafka`.
2. Создайте файл `requirements.txt` со следующим содержимым:
   ```
   kafka-python==2.0.2
   requests==2.31.0
   ```
3. Установите зависимости:
   ```
   pip install -r requirements.txt
   ```

### Шаг 2: Создание продюсера (producer.py)

Этот файл отправляет данные о погоде в Kafka.

#### Код:
```python
import json
from kafka import KafkaProducer
import requests
import time

API_KEY = "ВАШ_API_КЛЮЧ_ОТ_OPENWEATHERMAP"
CITIES = ["Moscow", "Saint Petersburg", "Krasnoyarsk", "Sochi", "Vladivostok"]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_weather(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()
    return {
        "city": city,
        "temperature": data["main"]["temp"],
        "condition": data["weather"][0]["description"]
    }

while True:
    for city in CITIES:
        weather_data = get_weather(city)
        producer.send("weather", weather_data)
        print(f"Sent: {weather_data}")
    time.sleep(60)  # Отправка данных каждые 60 секунд
```

### Шаг 3: Создание потребителя (consumer.py)

Этот файл обрабатывает данные из Kafka и сохраняет их в базу данных SQLite.

#### Код:
```python
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
```

### Шаг 4: Экспорт данных в JSON (export_to_json.py)

Этот файл выгружает данные из базы данных в JSON.

#### Код:
```python
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
```

## 3. Структура проекта
```
C:\weather-kafka
├── producer.py       # Отправка данных в Kafka
├── consumer.py       # Обработка данных из Kafka
├── export_to_json.py # Экспорт данных в JSON
├── weather.db        # SQLite база данных
├── requirements.txt  # Зависимости Python
```

## 4. Инструкция по запуску

1. Убедитесь, что Kafka и Zookeeper запущены.
2. Запустите продюсера:
   ```
   python producer.py
   ```
   Этот файл будет отправлять данные о погоде каждые 60 секунд.
3. В отдельном терминале запустите потребителя:
   ```
   python consumer.py
   ```
   Этот файл будет сохранять данные в базу SQLite.
4. Для экспорта данных в JSON выполните:
   ```
   python export_to_json.py
   ```

## 5. Структура базы данных

### Таблица `weather`
| Поле        	| Тип       | Описание                        |
|---------------|-----------|---------------------------------|
| `id`        	| INTEGER   | Уникальный идентификатор записи |
| `city`      	| TEXT      | Название города                 |
| `temperature` | REAL    	| Температура                     |
| `condition` 	| TEXT      | Описание погоды                 |
| `timestamp` 	| DATETIME  | Время сохранения записи         |

Пример содержимого таблицы:
| id | city          	| temperature | condition       			| timestamp           |
|----|------------------|-------------|-----------------------------|---------------------|
| 1  | Moscow        	| -5.08       | overcast clouds      		| 2024-12-19 20:24:31 |
| 2  | Saint Petersburg | 2.25     	  | light intensity shower rain | 2024-12-19 20:24:31 |
