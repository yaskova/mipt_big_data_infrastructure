import os
import logging  # Импортируем модуль для логирования
from pyspark.sql import SparkSession  # Импортируем SparkSession для создания и управления сессией Spark
from pyspark.sql.functions import from_json, col, expr, regexp_extract, concat_ws, split, column  # Импортируем функции PySpark для обработки данных
from pyspark.sql.types import StructType, IntegerType, FloatType, StructField  # Импортируем типы данных и схемы для структурированных данных

# Задаем параметры подключения к Kafka
KAFKA_BROKER = "kafka:9092"  # Адрес брокера Kafka
KAFKA_TOPIC = "customers"  # Название топика для чтения данных из Kafka

# Задаем параметры подключения к ClickHouse
# Параметры подключения к ClickHouse
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")  # Получаем хост ClickHouse из переменной среды
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))  # Порт ClickHouse
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")  # Название базы данных ClickHouse
CLICKHOUSE_TABLE = "customers"  # Название таблицы ClickHouse

# Драйвер для подключения к ClickHouse
driver = "com.clickhouse.jdbc.ClickHouseDriver"  # Указываем драйвер для ClickHouse JDBC

# Настраиваем логирование
logging.basicConfig(  # Настройка формата логов и уровня логирования
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)  # Создаем объект логера


def save_to_clickhouse(partition_data):  # Функция для сохранения данных в ClickHouse
    partition_data.show(truncate=False)  # Вывод данных в консоль для проверки
    try:
        # Настройка и выполнение записи данных в ClickHouse
        partition_data.write \
            .format("jdbc") \
            .option('driver', driver) \
            .option("url", f"jdbc:ch://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}") \
            .option("dbtable", CLICKHOUSE_TABLE) \
            .mode("append") \
            .save()
        logger.info(f"Данные успешно записаны в {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}.")
    except Exception as e:  # Обработка ошибок записи
        logger.error(f"Ошибка при записи данных в ClickHouse: {e}")


def main():  # Основная функция
    # Создаем сессию Spark
    spark = SparkSession.builder \
        .appName("Spark consumer") \
        .master("local[*]") \
        .getOrCreate()

    # Читаем данные из Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Парсинг JSON-данных из Kafka
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_data")  # Преобразуем сообщения Kafka в строки JSON
    df_extracted = parsed_df.select(  # Извлекаем отдельные поля из JSON-данных
        regexp_extract("json_data", r'"number":\s*([\d.]+)', 1).alias("number"),
        regexp_extract("json_data", r'"age":\s*([\d.]+)', 1).alias("age"),
        regexp_extract("json_data", r'"income":\s*([\d.]+)', 1).alias("income"),
        regexp_extract("json_data", r'"spending_score":\s*([\d.]+)', 1).alias("spending_score"),
        regexp_extract("json_data", r'"membership_years":\s*([\d.]+)', 1).alias("membership_years"),
        regexp_extract("json_data", r'"purchase_frequency":\s*([\d.]+)', 1).alias("purchase_frequency"),
        regexp_extract("json_data", r'"last_purchase_amount":\s*([\d.]+)', 1).alias("last_purchase_amount"),
    )
    # Формируем окончательный DataFrame
    df_final = df_extracted.select(
        concat_ws(" ", *[col for col in df_extracted.columns]).alias("space_separated_values")
    ).select(split(column("space_separated_values"), " ").alias("split_values")) \
    .select(
        column("split_values").getItem(0).cast(IntegerType()).alias("Number"),
        column("split_values").getItem(1).cast(IntegerType()).alias("Age"),
        column("split_values").getItem(2).cast(FloatType()).alias("Income"),
        column("split_values").getItem(3).cast(FloatType()).alias("Spending_Score"),
        column("split_values").getItem(4).cast(IntegerType()).alias("Membership_Years"),
        column("split_values").getItem(5).cast(FloatType()).alias("Purchase_Frequency"),
        column("split_values").getItem(6).cast(FloatType()).alias("Last_Purchase_Amount"),
    )
    # Настройка записи данных в ClickHouse с использованием foreachBatch
    df_final.writeStream \
        .foreachBatch(lambda df, _: save_to_clickhouse(df)) \
        .start() \
        .awaitTermination()

if __name__ == "__main__":  # Проверка на выполнение скрипта напрямую
    main()  # Запуск основной функции