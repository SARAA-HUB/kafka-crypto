# 🚀 Crypto Streaming Pipeline

> Pipeline de données en temps réel pour surveiller les prix des cryptomonnaies — construit avec Kafka, Spark, PostgreSQL et Grafana.

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.4-black)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Grafana](https://img.shields.io/badge/Grafana-latest-red)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)

---

## 📊 Architecture
CoinGecko API → Producer Python → Kafka → Spark Streaming → PostgreSQL → Grafana
---

## 💹 Cryptos surveillées

| Crypto | Symbole |
|--------|---------|
| Bitcoin | BTC |
| Ethereum | ETH |
| Solana | SOL |
| Cardano | ADA |
| Ripple | XRP |
| Dogecoin | DOGE |
| BNB | BNB |

---

## 🛠️ Stack technique

| Outil | Rôle |
|-------|------|
| Python | Producer et orchestration |
| Apache Kafka | Bus de messages temps réel |
| Apache Spark | Traitement streaming |
| PostgreSQL | Stockage des agrégations |
| Grafana | Visualisation et alertes |
| Docker | Conteneurisation |

---

## ⚡ Lancement rapide

### Prérequis
- Docker Desktop
- Python 3.11
- Java 17

### 1. Cloner le repo
---

## 💹 Cryptos surveillées

| Crypto | Symbole |
|--------|---------|
| Bitcoin | BTC |
| Ethereum | ETH |
| Solana | SOL |
| Cardano | ADA |
| Ripple | XRP |
| Dogecoin | DOGE |
| BNB | BNB |

---

## 🛠️ Stack technique

| Outil | Rôle |
|-------|------|
| Python | Producer et orchestration |
| Apache Kafka | Bus de messages temps réel |
| Apache Spark | Traitement streaming |
| PostgreSQL | Stockage des agrégations |
| Grafana | Visualisation et alertes |
| Docker | Conteneurisation |

---

## ⚡ Lancement rapide

### Prérequis
- Docker Desktop
- Python 3.11
- Java 17

### 1. Cloner le repo
git clone https://github.com/SARAA-HUB/kafka-crypto.git
cd kafka-crypto
### 2. Démarrer les services
docker-compose up -d
### 3. Installer les dépendances Python
python -m venv venv311
venv311\Scripts\activate
pip install pyspark==3.5.0 psycopg2-binary kafka-python requests
### 4. Créer le topic Kafka
docker exec kafka-crypto-kafka-1 kafka-topics --create --topic crypto-prices --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
### 5. Lancer le producer
python producer/producer.py
### 6. Lancer Spark Streaming
### 7. Ouvrir Grafana
http://localhost:3001
Login: admin / admin
---

## 📈 Données collectées

Pour chaque crypto toutes les minutes :

| Colonne | Description |
|---------|-------------|
| prix_moyen | Prix moyen sur la fenêtre |
| prix_min | Prix minimum observé |
| prix_max | Prix maximum observé |
| variation_moyenne | Variation moyenne sur 24h |
| nb_messages | Nombre de messages reçus |
| window_start | Début de la fenêtre |
| window_end | Fin de la fenêtre |

---

## 📁 Structure du projet
kafka-crypto/
├── docker-compose.yaml
├── producer/
│   └── producer.py
├── consumer/
│   └── spark_streaming.py
├── grafana/
│   └── dashboard.json
└── README.md

---

## 👩‍💻 Auteur

SARAA-HUB — https://github.com/SARAA-HUB