# Crypto Streaming Pipeline 🚀

Pipeline de données en temps réel pour les cryptomonnaies.

## Architecture
CoinGecko API → Producer → Kafka → Spark Streaming → PostgreSQL → Grafana

## Stack technique
- Apache Kafka
- Apache Spark Streaming
- PostgreSQL
- Grafana
- Docker
- Python

## Lancement
```bash
docker-compose up -d
python producer/producer.py
python consumer/spark_streaming.py
```