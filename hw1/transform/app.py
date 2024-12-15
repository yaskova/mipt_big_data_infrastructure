import logging  # Импортируем модуль для логирования
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, lit
import time


# Параметры подключения к ClickHouse
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")  # Получаем хост ClickHouse из переменной среды
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))  # Порт ClickHouse
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")  # Название базы данных ClickHouse
CLICKHOUSE_SOURCE_TABLE =  "customers"  # Таблица для чтения
CLICKHOUSE_DEST_TABLE =  "customers_transformed"  # Таблица для записи

# Драйвер для подключения к ClickHouse
driver = "com.clickhouse.jdbc.ClickHouseDriver"  # Указываем драйвер для ClickHouse JDBC
# Настраиваем логирование
logging.basicConfig(  # Настройка формата логов и уровня логирования
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)  # Создаем объект логера    


# Инициализация SparkSession
spark = SparkSession.builder \
        .appName("Spark ClickHouse Integration") \
        .getOrCreate()



def save_to_clickhouse(dataframe):
    """Сохранение данных в ClickHouse с обновлением."""
    try:
        dataframe.show(truncate=False)  # Вывод данных в консоль для проверки        
        # Добавляем колонку UpdateVersion с текущим временем
        dataframe_with_version = dataframe.withColumn("UpdateVersion", lit(int(time.time())))

        # Запись данных в ClickHouse
        dataframe_with_version.write \
            .format("jdbc") \
            .option("driver", driver) \
            .option("url", f"jdbc:ch://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}") \
            .option("dbtable", CLICKHOUSE_DEST_TABLE) \
            .mode("append") \
            .save()

        logger.info(f"Данные успешно записаны в таблицу {CLICKHOUSE_DEST_TABLE} с обновлением.")
    except Exception as e:
        logger.error(f"Ошибка при записи данных в ClickHouse: {e}")
        

def process_data():
    try:
        # Чтение данных из таблицы ClickHouse
        customers_df = spark.read \
            .format("jdbc") \
            .option('driver', driver) \
            .option("url", f"jdbc:ch://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}") \
            .option("dbtable", CLICKHOUSE_SOURCE_TABLE) \
            .load()
        logger.info(f"Данные успешно считаны из {CLICKHOUSE_DATABASE}.{CLICKHOUSE_SOURCE_TABLE}.")
        
        # Преобразование данных: расчет среднего дохода для каждого возраста
        avg_income_by_age_df = customers_df \
            .filter(customers_df["Income"].isNotNull() & customers_df["Age"].isNotNull()) \
            .groupBy("Age") \
            .agg(avg("Income").alias("AverageIncome"))

        # Проверка структуры данных и преобразование типов для совместимости
        avg_income_by_age_df = avg_income_by_age_df.selectExpr(
            "cast(Age as INT) as Age",
            "cast(AverageIncome as DOUBLE) as AverageIncome"
        )
        logger.info(f"Данные успешно преобразованы.")
        
        # Запись результатов в новую таблицу ClickHouse
        save_to_clickhouse(avg_income_by_age_df)
    except Exception as e:
        logger.error(f"Ошибка в процессе обработки данных: {e}")        

def main():    
    try:
        logger.info("Запуск приложения для периодического пересчёта данных.")
        while True:
            process_data()
            logger.info("Ожидание 1 минуты до следующего пересчёта.")
            time.sleep(60)  # Ждём 1 минуту #TODO пробросить статус таблицы клиентов и кроме правильно подобранного времени учитывать необходимость пересчета
    finally:
        spark.stop()
        logger.info("SparkSession успешно завершён.")


if __name__ == "__main__":
    main()