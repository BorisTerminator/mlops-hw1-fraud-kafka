"""Читает результаты скоринга из топика scores и складывает их в витрину Postgres."""
import json
import logging
import os
import signal
import time

import psycopg
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('score_writer')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
SCORES_TOPIC = os.getenv('KAFKA_SCORES_TOPIC', 'scores')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '500'))
DSN = (
    f"host={os.getenv('POSTGRES_HOST', 'postgres')} "
    f"port={os.getenv('POSTGRES_PORT', '5432')} "
    f"dbname={os.getenv('POSTGRES_DB', 'fraud')} "
    f"user={os.getenv('POSTGRES_USER', 'fraud')} "
    f"password={os.getenv('POSTGRES_PASSWORD', 'fraud')}"
)

INSERT_SQL = """
    INSERT INTO fraud_scores (transaction_id, score, fraud_flag)
    VALUES (%s, %s, %s)
    ON CONFLICT (transaction_id) DO NOTHING
"""

running = True


def stop(signum, frame):
    global running
    logger.info('Got signal %s, stopping...', signum)
    running = False


def connect():
    while True:
        try:
            return psycopg.connect(DSN)
        except psycopg.OperationalError as e:
            logger.warning('Postgres is not ready (%s), retry in 2s', e)
            time.sleep(2)


def parse(messages):
    rows = []
    for msg in messages:
        if msg.error():
            logger.error('Kafka error: %s', msg.error())
            continue
        try:
            r = json.loads(msg.value())
            rows.append((str(r['transaction_id']), float(r['score']), int(r['fraud_flag'])))
        except (ValueError, KeyError, TypeError) as e:
            logger.warning('Skip malformed message at offset %s: %s', msg.offset(), e)
    return rows


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    conn = connect()
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'score-writer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    consumer.subscribe([SCORES_TOPIC])
    logger.info('Listening %s on %s', SCORES_TOPIC, KAFKA_BOOTSTRAP_SERVERS)

    try:
        while running:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
            if not messages:
                continue
            rows = parse(messages)
            if rows:
                with conn.transaction(), conn.cursor() as cur:
                    cur.executemany(INSERT_SQL, rows)
            # Оффсеты фиксируем после записи в БД; повтор не создаст дублей благодаря ON CONFLICT
            consumer.commit(asynchronous=False)
            logger.info('Saved %d scores', len(rows))
    finally:
        consumer.close()
        conn.close()


if __name__ == '__main__':
    main()
