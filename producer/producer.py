from kafka import KafkaProducer
import requests
import json
import time
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

CRYPTOS = "bitcoin,ethereum,solana,binancecoin,cardano,ripple,dogecoin"
KAFKA_TOPIC = "crypto-prices"
KAFKA_SERVER = "127.0.0.1:9092"
INTERVAL = 30

def fetch_prices():
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": CRYPTOS,
            "vs_currencies": "usd",
            "include_24hr_change": "true",
            "include_market_cap": "true",
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log.error(f"Erreur API: {e}")
        return None

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )
    log.info(f"Producer démarré — envoi toutes les {INTERVAL}s")

    while True:
        data = fetch_prices()
        if data:
            for coin, values in data.items():
                message = {
                    "coin": coin,
                    "prix_usd": values["usd"],
                    "variation_24h": round(values.get("usd_24h_change", 0), 2),
                    "market_cap": values.get("usd_market_cap", 0),
                    "timestamp": int(time.time() * 1000)
                }
                producer.send(
                    KAFKA_TOPIC,
                    key=coin,
                    value=message
                )
                log.info(f"Envoyé: {coin} → ${values['usd']}")
            producer.flush()
            log.info(f"Batch envoyé — prochain dans {INTERVAL}s")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()