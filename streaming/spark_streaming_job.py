"""Spark Structured Streaming job: validasi, routing, dan monitoring transaksi.

Alur pipeline
-------------
Kafka ``transactions``
    -> parse JSON (semua field dibaca sebagai string, lalu divalidasi eksplisit)
    -> 5 validasi wajib + pengecekan late event + pengecekan duplicate
    -> Kafka ``transactions_valid``  (event valid)
    -> Kafka ``transactions_dlq``    (event invalid / late / duplicate)

Dua streaming query berjalan bersamaan:
    1. routing    - foreachBatch: validasi, routing, running total di console
    2. monitoring - withWatermark("event_time", "3 minutes") + tumbling window
                    1 menit, total transaksi per window di console
"""

import os

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

TOPIC_INPUT = "transactions"
TOPIC_VALID = "transactions_valid"
TOPIC_DLQ = "transactions_dlq"

ALLOWED_SOURCES = ["mobile", "web", "pos"]
AMOUNT_MIN = 1
AMOUNT_MAX = 10_000_000
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

WATERMARK_DELAY = "3 minutes"
WATERMARK_SECONDS = 180
WINDOW_SIZE = "1 minute"


RAW_SCHEMA = StructType(
    [
        StructField("user_id", StringType()),
        StructField("amount", StringType()),
        StructField("timestamp", StringType()),
        StructField("source", StringType()),
    ]
)
SEEN_KEYS_SCHEMA = StructType(
    [StructField("user_id", StringType()), StructField("event_time", TimestampType())]
)


def create_state(spark: SparkSession) -> dict:
    """Membuat state di sisi driver yang dipakai bersama antar micro-batch query routing."""
    return {
        "max_event_epoch": None,  
        "running_total": 0,  
        "running_amount": 0.0,  
        "seen_keys": spark.createDataFrame([], SEEN_KEYS_SCHEMA),
    }


def get_watermark_epoch(state: dict):
    """Watermark saat ini = event time terbaru - 3 menit (None di awal)."""
    if state["max_event_epoch"] is None:
        return None
    return state["max_event_epoch"] - WATERMARK_SECONDS


def is_blank(column: Column) -> Column:
    """True jika kolom string bernilai null atau hanya berisi spasi."""
    return column.isNull() | (F.trim(column) == "")


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Membaca stream transaksi mentah dari Kafka."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_INPUT)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_validate(raw: DataFrame) -> DataFrame:
    """Deserialize JSON dan menjalankan validasi yang tidak membutuhkan state.

    Menambahkan kolom ``amount_num``, ``event_time`` dan ``error_reason``
    (null = valid sejauh ini). Alasan dievaluasi berurutan sesuai prioritas,
    aturan pertama yang gagal akan dipakai.
    """
    payload = F.col("value").cast("string")
    parsed = raw.select(
        payload.alias("raw_payload"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.from_json(payload, RAW_SCHEMA).alias("data"),
    ).select(
        "raw_payload",
        "kafka_partition",
        "kafka_offset",
        F.col("data.user_id").alias("user_id"),
        F.col("data.amount").alias("amount_raw"),
        F.col("data.timestamp").alias("timestamp_raw"),
        F.col("data.source").alias("source"),
    )

    typed = parsed.withColumn("amount_num", F.col("amount_raw").cast("double")).withColumn(
        "event_time", F.to_timestamp(F.col("timestamp_raw"), TIMESTAMP_FORMAT)
    )

    all_fields_null = (
        F.col("user_id").isNull()
        & F.col("amount_raw").isNull()
        & F.col("timestamp_raw").isNull()
        & F.col("source").isNull()
    )
    missing_field = (
        is_blank(F.col("user_id"))
        | is_blank(F.col("amount_raw"))
        | is_blank(F.col("timestamp_raw"))
    )
    bad_amount_type = F.col("amount_num").isNull() | F.isnan(F.col("amount_num"))
    out_of_range = (F.col("amount_num") < AMOUNT_MIN) | (F.col("amount_num") > AMOUNT_MAX)
    unknown_source = ~F.coalesce(F.col("source").isin(*ALLOWED_SOURCES), F.lit(False))

    error_reason = (
        F.when(all_fields_null, "MALFORMED_PAYLOAD") 
        .when(missing_field, "MISSING_FIELD")  
        .when(bad_amount_type, "INVALID_TYPE_AMOUNT")  
        .when(F.col("event_time").isNull(), "INVALID_TYPE_TIMESTAMP")  
        .when(out_of_range, "AMOUNT_OUT_OF_RANGE")  
        .when(unknown_source, "UNKNOWN_SOURCE")  
    )
    return typed.withColumn("error_reason", error_reason)


def flag_late_events(df: DataFrame, watermark_epoch) -> DataFrame:
    """Menandai event yang masih valid tetapi lebih tua dari watermark sebagai LATE_EVENT."""
    if watermark_epoch is None:
        return df
    is_late = F.col("error_reason").isNull() & (
        F.col("event_time").cast("long") < F.lit(watermark_epoch)
    )
    return df.withColumn(
        "error_reason", F.when(is_late, "LATE_EVENT").otherwise(F.col("error_reason"))
    )


def flag_duplicates(df: DataFrame, seen_keys: DataFrame) -> DataFrame:
    """Menandai duplicate (user_id + event_time) di dalam batch maupun lintas batch."""
    df = df.withColumn("is_candidate", F.col("error_reason").isNull())
    ordering = Window.partitionBy("user_id", "event_time", "is_candidate").orderBy(
        "kafka_partition", "kafka_offset"
    )
    seen = seen_keys.withColumn("seen_before", F.lit(True))

    is_duplicate = F.col("is_candidate") & (
        (F.col("occurrence") > 1) | F.col("seen_before").isNotNull()
    )
    return (
        df.withColumn("occurrence", F.row_number().over(ordering))
        .join(seen, on=["user_id", "event_time"], how="left")
        .withColumn(
            "error_reason",
            F.when(is_duplicate, "DUPLICATE_EVENT").otherwise(F.col("error_reason")),
        )
        .withColumn("is_valid", F.col("error_reason").isNull())
    )


def collect_batch_stats(df: DataFrame):
    """Menghitung semua counter batch dalam satu Spark job."""
    epoch = F.col("event_time").cast("long")
    return df.agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("is_valid"), 1).otherwise(0)).alias("valid_count"),
        F.sum(F.when(F.col("is_valid"), F.col("amount_num")).otherwise(0.0)).alias(
            "valid_amount"
        ),
        F.sum(F.when(F.col("error_reason") == "LATE_EVENT", 1).otherwise(0)).alias(
            "late_count"
        ),
        F.sum(F.when(F.col("error_reason") == "DUPLICATE_EVENT", 1).otherwise(0)).alias(
            "duplicate_count"
        ),
        F.max(F.when(F.col("is_candidate"), epoch)).alias("max_epoch"),
    ).first()


def write_to_kafka(df: DataFrame, topic: str, columns: list) -> None:
    """Mengubah kolom terpilih menjadi JSON lalu mempublikasikannya ke topic Kafka."""
    (
        df.select(
            F.col("user_id").alias("key"),
            F.to_json(F.struct(*columns)).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", topic)
        .save()
    )


def route_to_kafka(valid_df: DataFrame, invalid_df: DataFrame) -> None:
    """Mengirim event valid ke transactions_valid dan sisanya ke DLQ."""
    write_to_kafka(
        valid_df,
        TOPIC_VALID,
        [
            F.col("user_id"),
            F.col("amount_num").alias("amount"),
            F.col("timestamp_raw").alias("timestamp"),
            F.col("source"),
            F.col("is_valid"),
        ],
    )
    write_to_kafka(
        invalid_df,
        TOPIC_DLQ,
        [
            F.col("user_id"),
            F.col("amount_raw").alias("amount"),
            F.col("timestamp_raw").alias("timestamp"),
            F.col("source"),
            F.col("is_valid"),
            F.col("error_reason"),
            F.col("raw_payload"),
        ],
    )


def update_state(state: dict, stats, valid_df: DataFrame) -> None:
    """Memperbarui running total, watermark, dan penyimpanan key yang sudah terlihat."""
    state["running_total"] += int(stats["valid_count"] or 0)
    state["running_amount"] += float(stats["valid_amount"] or 0.0)
    if stats["max_epoch"] is not None:
        state["max_event_epoch"] = max(state["max_event_epoch"] or 0, int(stats["max_epoch"]))

    keys = state["seen_keys"].union(valid_df.select("user_id", "event_time"))
    new_watermark = get_watermark_epoch(state)
    if new_watermark is not None:
        keys = keys.filter(F.col("event_time").cast("long") >= new_watermark)
    state["seen_keys"] = keys.localCheckpoint()


def print_summary(spark: SparkSession, state: dict, stats, batch_id: int) -> None:
    """Mencetak output console yang diminta assignment."""
    invalid_count = int(stats["total_count"]) - int(stats["valid_count"])
    (
        spark.range(1)
        .select(
            F.current_timestamp().alias("timestamp"),
            F.lit(state["running_total"]).alias("running_total"),
            F.lit(state["running_amount"]).alias("running_amount"),
            F.lit(batch_id).alias("batch_id"),
            F.lit(int(stats["valid_count"])).alias("valid_in_batch"),
            F.lit(invalid_count).alias("invalid_in_batch"),
            F.lit(int(stats["late_count"])).alias("late_in_batch"),
            F.lit(int(stats["duplicate_count"])).alias("duplicate_in_batch"),
        )
        .show(truncate=False)
    )


def process_batch(batch_df: DataFrame, batch_id: int, state: dict) -> None:
    """Handler foreachBatch: menandai, merouting, dan melaporkan satu micro-batch."""
    if batch_df.isEmpty():
        return

    spark = batch_df.sparkSession
    flagged = flag_late_events(batch_df, get_watermark_epoch(state))
    df = flag_duplicates(flagged, state["seen_keys"]).persist()

    try:
        valid_df = df.filter(F.col("is_valid"))
        invalid_df = df.filter(~F.col("is_valid"))

        stats = collect_batch_stats(df)
        route_to_kafka(valid_df, invalid_df)

        if stats["late_count"]:
            print(
                f"[LATE EVENT DETECTED] batch={batch_id} count={stats['late_count']} "
                f"(older than watermark of {WATERMARK_DELAY})"
            )
        if stats["total_count"] != stats["valid_count"]:
            invalid_df.select(
                "user_id", "amount_raw", "timestamp_raw", "source", "error_reason"
            ).show(truncate=False)

        update_state(state, stats, valid_df)
        print_summary(spark, state, stats, batch_id)
    finally:
        df.unpersist()


def process_monitoring_batch(window_df: DataFrame, batch_id: int) -> None:
    """Handler foreachBatch untuk query monitoring berbasis window."""
    if window_df.isEmpty():
        return
    (
        window_df.select(
            F.current_timestamp().alias("timestamp"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_transactions",
            "total_amount",
        )
        .orderBy("window_start")
        .show(truncate=False)
    )


def build_monitoring_stream(validated: DataFrame) -> DataFrame:
    """Watermark + dedupe bawaan Spark + tumbling window 1 menit untuk total per window."""
    return (
        validated.filter(F.col("error_reason").isNull())
        .withWatermark("event_time", WATERMARK_DELAY)
        .dropDuplicates(["user_id", "event_time"])
        .groupBy(F.window("event_time", WINDOW_SIZE))
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount_num").alias("total_amount"),
        )
    )


def create_spark_session() -> SparkSession:
    """Membuat SparkSession lokal dengan package Kafka dan zona waktu UTC."""
    spark = (
        SparkSession.builder.appName("transactions-streaming-pipeline")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    """Menjalankan query routing dan monitoring secara paralel."""
    spark = create_spark_session()
    validated = parse_and_validate(read_kafka_stream(spark))
    state = create_state(spark)

    routing_query = (
        validated.writeStream.queryName("routing")
        .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, state))
        .option("checkpointLocation", "checkpoints/routing")
        .trigger(processingTime="5 seconds")
        .start()
    )
    monitoring_query = (
        build_monitoring_stream(validated)
        .writeStream.queryName("monitoring")
        .outputMode("update")
        .foreachBatch(process_monitoring_batch)
        .option("checkpointLocation", "checkpoints/monitoring")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(f"Running queries: {routing_query.name}, {monitoring_query.name}")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
