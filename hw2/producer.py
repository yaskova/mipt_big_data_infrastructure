import json
from kafka import KafkaProducer
import requests
import time

API_KEY = "c93d8536110cc9b92427499b4baae447"
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