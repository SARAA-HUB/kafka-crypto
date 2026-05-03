from kafka import KafkaConsumer
import json
import psycopg2
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

KAFKA_TOPIC  = "crypto-prices"
KAFKA_SERVER = "127.0.0.1:9092"

def get_db():
    return psycopg2.connect(
        host="localhost", port=5433,
        database="crypto_db",
        user="crypto", password="crypto"
    )

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crypto_streaming (
            id SERIAL PRIMARY KEY,
            coin VARCHAR(50),
            prix_usd FLOAT,
            variation_24h FLOAT,
            market_cap FLOAT,
            date TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    log.info("Table crypto_streaming créée ✅")

def main():
    init_db()
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='crypto-consumer'
    )
    log.info("Consumer démarré — en attente de messages...")

    conn = get_db()
    cur = conn.cursor()

    for message in consumer:
        data = message.value
        log.info(f"Reçu: {data['coin']} → ${data['prix_usd']}")
        try:
            cur.execute("""
                INSERT INTO crypto_streaming
                (coin, prix_usd, variation_24h, market_cap, date)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                data['coin'],
                data['prix_usd'],
                data['variation_24h'],
                data['market_cap'],
                datetime.now()
            ))
            conn.commit()
        except Exception as e:
            log.error(f"Erreur DB: {e}")
            conn.rollback()

if __name__ == "__main__":
    main()