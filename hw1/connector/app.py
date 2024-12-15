import os
from flask import Flask, jsonify
import psycopg2
from kafka import KafkaProducer
import simplejson
import json

# Инициализация Flask приложения
app = Flask(__name__)

# Конфигурация PostgreSQL из переменных окружения
PG_CONFIG = {
    "dbname": os.getenv("PG_DBNAME", "postgres"),  # Имя базы данных
    "user": os.getenv("PG_USER", "myuser"),  # Имя пользователя
    "password": os.getenv("PG_PASSWORD", "mypassword"),  # Пароль
    "host": os.getenv("PG_HOST", "postgres-service"),  # Хост базы данных
    "port": int(os.getenv("PG_PORT", 5432))  # Порт базы данных
}

# Конфигурация Kafka из переменных окружения
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),  # Адрес Kafka
    "topic": os.getenv("KAFKA_TOPIC", "customers")  # Тема Kafka
}

def fetch_customers():
    """Получение данных о клиентах из PostgreSQL."""
    print("Получение данных о клиентах из PostgreSQL.")
    connection = psycopg2.connect(**PG_CONFIG)  # Установка соединения с PostgreSQL
    cursor = connection.cursor()  # Создание курсора
    cursor.execute("SELECT * FROM customers;")  # Выполнение SQL-запроса
    rows = cursor.fetchall()  # Получение всех строк из результата запроса
    columns = [desc[0] for desc in cursor.description]  # Получение имен колонок
    cursor.close()  # Закрытие курсора
    connection.close()  # Закрытие соединения
    return [dict(zip(columns, row)) for row in rows]  # Формирование списка словарей

def push_to_kafka(customers):
    """Отправка данных о клиентах в Kafka."""
    print("Отправка данных о клиентах в Kafka.")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],  # Настройка Kafka
        value_serializer=lambda v: simplejson.dumps(v).encode("utf-8")  # Сериализация данных в JSON
    )
    for customer in customers:
        producer.send(KAFKA_CONFIG["topic"], customer)  # Отправка каждого клиента в Kafka
    producer.flush()  # Ожидание завершения отправки

@app.route("/fetch", methods=["GET"])
def fetch_and_push():
    """Получение данных из PostgreSQL и отправка в Kafka."""
    print("Получение данных из PostgreSQL и отправка в Kafka.")
    try:
        customers = fetch_customers()  # Получение данных о клиентах
        if customers:
            push_to_kafka(customers)  # Отправка данных в Kafka
            return jsonify({
                "message": f"Отправлено {str(len(customers))} записей в Kafka.",
                "data": customers
            }), 200
        else:
            return jsonify({"message": "Записей о клиентах не найдено."}), 404
    except Exception as e:
        return ({"error": str(e)}), 500  # Обработка ошибок

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("APP_PORT", 5000)))  # Запуск приложения