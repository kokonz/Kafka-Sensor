from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import signal
import time

temp_schema = StructType([
    StructField("gudang_id", StringType()),
    StructField("suhu", IntegerType())
])

humidity_schema = StructType([
    StructField("gudang_id", StringType()),
    StructField("kelembaban", IntegerType())
])

spark = None
query_individual = None
query_combined = None

def graceful_shutdown(signum, frame):
    print("\n[SHUTDOWN] Menghentikan streaming query...")
    try:
        if query_combined is not None and query_combined.isActive:
            query_combined.stop()
            print("[SHUTDOWN] Query gabungan dihentikan")

        if query_individual is not None and query_individual.isActive:
            query_individual.stop()
            print("[SHUTDOWN] Query individu dihentikan")

    except Exception as e:
        print(f"[SHUTDOWN ERROR] {str(e)}")

    finally:
        if spark is not None:
            spark.stop()
            print("[SHUTDOWN] Spark session dihentikan")
        sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    try:
        spark = SparkSession.builder \
            .appName("WarehouseMonitoring") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoints/warehouse_monitoring") \
            .config("spark.driver.memory", "2g") \
            .config("spark.driver.maxResultSize", "1g") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.ui.showConsoleProgress", "false") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        temp_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "sensor-suhu-gudang") \
            .option("startingOffsets", "latest") \
            .load() \
            .selectExpr("CAST(value AS STRING)", "timestamp") \
            .select(
                from_json(col("value"), temp_schema).alias("data"),
                col("timestamp")
            ) \
            .select("data.*", "timestamp")

        humidity_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "sensor-kelembaban-gudang") \
            .option("startingOffsets", "latest") \
            .load() \
            .selectExpr("CAST(value AS STRING)", "timestamp") \
            .select(
                from_json(col("value"), humidity_schema).alias("data"),
                col("timestamp")
            ) \
            .select("data.*", "timestamp")

        temp_alerts = temp_stream.filter(col("suhu") > 80) \
            .select(
                lit("[PERINGATAN SUHU TINGGI]").alias("alert_type"),
                col("gudang_id"),
                col("suhu").alias("nilai")
            )

        humidity_alerts = humidity_stream.filter(col("kelembaban") > 70) \
            .select(
                lit("[PERINGATAN KELEMBABAN TINGGI]").alias("alert_type"),
                col("gudang_id"),
                col("kelembaban").alias("nilai")
            )

        combined_alerts = temp_alerts.union(humidity_alerts)

        query_individual = combined_alerts.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime="5 seconds") \
            .start()

        temp_windowed = temp_stream \
            .withWatermark("timestamp", "5 seconds") \
            .groupBy(
                window(col("timestamp"), "10 seconds"),
                col("gudang_id")
            ) \
            .agg(max("suhu").alias("max_suhu"))

        humidity_windowed = humidity_stream \
            .withWatermark("timestamp", "5 seconds") \
            .groupBy(
                window(col("timestamp"), "10 seconds"),
                col("gudang_id")
            ) \
            .agg(max("kelembaban").alias("max_kelembaban"))

        joined_stream = temp_windowed.alias("temp").join(
            humidity_windowed.alias("humidity"),
            (col("temp.gudang_id") == col("humidity.gudang_id")) &
            (col("temp.window") == col("humidity.window")),
            "inner"
        ).select(
            col("temp.gudang_id"),
            col("temp.window"),
            col("temp.max_suhu"),
            col("humidity.max_kelembaban")
        )

        status_stream = joined_stream.withColumn("status",
            when(
                (col("max_suhu") > 80) & (col("max_kelembaban") > 70),
                "BAHAYA TINGGI! Barang berisiko rusak"
            ).when(
                col("max_suhu") > 80,
                "Suhu tinggi, kelembaban normal"
            ).when(
                col("max_kelembaban") > 70,
                "Kelembaban tinggi, suhu aman"
            ).otherwise("Aman")
        )

        formatted_status = status_stream.select(
            lit("[PERINGATAN KRITIS]").alias("alert_type"),
            col("gudang_id"),
            col("max_suhu").alias("suhu"),
            col("max_kelembaban").alias("kelembaban"),
            col("status")
        )

        query_combined = formatted_status.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime="10 seconds") \
            .start()

        print("\n[SYSTEM] Streaming query berjalan...")
        print("[SYSTEM] Tekan Ctrl+C untuk menghentikan\n")

        time.sleep(5)

        spark.streams.awaitAnyTermination()

    except Exception as e:
        print(f"\n[ERROR] Terjadi kesalahan: {str(e)}")
    finally:
        graceful_shutdown(None, None)
