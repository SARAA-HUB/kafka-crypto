import os
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.18.8-hotspot'

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, min, max, count, window, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
import psycopg2

schema = StructType([
    StructField('coin',          StringType(), True),
    StructField('prix_usd',      FloatType(),  True),
    StructField('variation_24h', FloatType(),  True),
    StructField('market_cap',    FloatType(),  True),
    StructField('timestamp',     LongType(),   True),
])

spark = SparkSession.builder\
    .appName('CryptoStreaming')\
    .config('spark.jars.packages',
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
            'org.postgresql:postgresql:42.6.0')\
    .config('spark.sql.streaming.checkpointLocation', 'C:/tmp/spark-checkpoint')\
    .config('spark.hadoop.fs.file.impl', 'org.apache.hadoop.fs.LocalFileSystem')\
    .config('spark.hadoop.fs.file.impl.disable.cache', 'true')\
    .config('spark.sql.shuffle.partitions', '2')\
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df_raw = spark.readStream\
    .format('kafka')\
    .option('kafka.bootstrap.servers', '127.0.0.1:9092')\
    .option('subscribe', 'crypto-prices')\
    .option('startingOffsets', 'latest')\
    .load()

df = df_raw.select(
    from_json(col('value').cast('string'), schema).alias('data')
).select('data.*')\
 .withColumn('event_time', to_timestamp(col('timestamp') / 1000))

df_agg = df\
    .withWatermark('event_time', '1 minute')\
    .groupBy(
        window(col('event_time'), '1 minute'),
        col('coin')
    ).agg(
        avg('prix_usd').alias('prix_moyen'),
        min('prix_usd').alias('prix_min'),
        max('prix_usd').alias('prix_max'),
        avg('variation_24h').alias('variation_moyenne'),
        count('*').alias('nb_messages')
    )

def save_batch(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return
    conn = psycopg2.connect(
        host='localhost', port=5433,
        database='crypto_db',
        user='crypto', password='crypto'
    )
    cur = conn.cursor()
    cur.execute(
        'CREATE TABLE IF NOT EXISTS crypto_spark ('
        'id SERIAL PRIMARY KEY, coin VARCHAR(50), '
        'prix_moyen FLOAT, prix_min FLOAT, prix_max FLOAT, '
        'variation_moyenne FLOAT, nb_messages INT, '
        'window_start TIMESTAMP, window_end TIMESTAMP)'
    )
    for row in rows:
        cur.execute(
            'INSERT INTO crypto_spark '
            '(coin, prix_moyen, prix_min, prix_max, variation_moyenne, nb_messages, window_start, window_end) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
            (row.coin, row.prix_moyen, row.prix_min, row.prix_max,
             row.variation_moyenne, row.nb_messages,
             row.window.start, row.window.end)
        )
    conn.commit()
    cur.close()
    conn.close()
    print(f'Batch {batch_id} sauvegarde - {len(rows)} lignes')

query = df_agg.writeStream\
    .foreachBatch(save_batch)\
    .outputMode('update')\
    .trigger(processingTime='30 seconds')\
    .start()

print('Spark Streaming demarre !')
query.awaitTermination()
