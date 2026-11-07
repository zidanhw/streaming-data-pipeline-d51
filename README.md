# Streaming for Data Pipeline

Pipeline real-time transaksi: producer mengirim event ke Kafka, lalu PySpark Structured
Streaming memvalidasi dan memisahkan data valid dan invalid (DLQ).

```
producer.py -> Kafka: transactions -> Spark Streaming -> transactions_valid
                                                      -> transactions_dlq
```

## Struktur

```
producer/producer.py              # event producer
streaming/spark_streaming_job.py  # streaming job PySpark
docker-compose.yml                # Kafka
screenshots/                      # bukti screenshot
```

## Cara Menjalankan

```bash
pip install -r requirements.txt
docker compose up -d

# buat topic
for t in transactions transactions_valid transactions_dlq; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --topic $t --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
done

# terminal A: streaming job (jalankan lebih dulu)
python streaming/spark_streaming_job.py

# terminal B: producer (60 event, 1-2 detik per event)
python producer/producer.py
```

Untuk mengulang dari awal, hapus folder `checkpoints/`.

## Fitur

**Producer:** event JSON setiap 1-2 detik, dengan event invalid (amount negatif/terlalu besar,
timestamp rusak, source tidak dikenal, field hilang, tipe salah, duplicate) dan 3 late event.

**Validasi (kolom `is_valid` dan `error_reason`):**
mandatory field, tipe data, range amount (1 - 10.000.000), source (`mobile`, `web`, `pos`),
dan duplicate (`user_id` + `timestamp`).

**Routing:** valid ke `transactions_valid`, invalid ke `transactions_dlq`.

**Watermark dan window:** `withWatermark("event_time", "3 minutes")` dan tumbling window
1 menit untuk total transaksi per window. Event lebih lama dari 3 menit ditandai
`LATE_EVENT` dan dikirim ke DLQ.

**Output console:** kolom `timestamp` dan `running_total` (total kumulatif transaksi valid).

## Screenshots

- `screenshots/producer.png`
- `screenshots/streaming_console.png`
- `screenshots/topic_valid.png`
- `screenshots/topic_dlq.png`