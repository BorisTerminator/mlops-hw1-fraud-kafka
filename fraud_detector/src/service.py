"""Kafka-сервис скоринга: читает транзакции из топика, пишет скор и флаг фрода в другой топик.

Входное сообщение:  {"transaction_id": "...", "data": {<строка test.csv>}}
Выходное сообщение: {"transaction_id": "...", "score": 0.0123, "fraud_flag": 0}
"""
import json
import logging
import os
import signal

import pandas as pd
from confluent_kafka import Consumer, Producer

from preprocessing import Preprocessor
from scorer import Scorer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('fraud_detector')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TRANSACTIONS_TOPIC = os.getenv('KAFKA_TRANSACTIONS_TOPIC', 'transactions')
SCORES_TOPIC = os.getenv('KAFKA_SCORES_TOPIC', 'scores')
MODEL_PATH = os.getenv('MODEL_PATH', 'models/my_catboost.cbm')
ARTIFACTS_PATH = os.getenv('ARTIFACTS_PATH', 'artifacts/preprocessing.json')
THRESHOLD = float(os.getenv('MODEL_THRESHOLD', '0.98'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '500'))

running = True


def stop(signum, frame):
    global running
    logger.info('Got signal %s, stopping...', signum)
    running = False


def parse(messages):
    ids, rows = [], []
    for msg in messages:
        if msg.error():
            logger.error('Kafka error: %s', msg.error())
            continue
        try:
            payload = json.loads(msg.value())
            ids.append(str(payload['transaction_id']))
            rows.append(payload['data'])
        except (ValueError, KeyError, TypeError) as e:
            logger.warning('Skip malformed message at offset %s: %s', msg.offset(), e)
    return ids, rows


def score_batch(preprocessor, scorer, ids, rows):
    features = preprocessor.transform(pd.DataFrame(rows))
    scores, flags = scorer.predict(features)
    return [
        {'transaction_id': tid, 'score': float(s), 'fraud_flag': int(f)}
        for tid, s, f in zip(ids, scores, flags)
    ]


def score(preprocessor, scorer, ids, rows):
    try:
        return score_batch(preprocessor, scorer, ids, rows)
    except Exception:
        # Одна битая строка не должна ронять весь батч — скорим по одной
        logger.exception('Batch scoring failed, falling back to one-by-one')
    results = []
    for tid, row in zip(ids, rows):
        try:
            results.extend(score_batch(preprocessor, scorer, [tid], [row]))
        except Exception as e:
            logger.error('Skip transaction %s: %s', tid, e)
    return results


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    preprocessor = Preprocessor(ARTIFACTS_PATH)
    scorer = Scorer(MODEL_PATH, THRESHOLD)

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'ml-scorer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    consumer.subscribe([TRANSACTIONS_TOPIC])
    logger.info('Listening %s -> %s on %s', TRANSACTIONS_TOPIC, SCORES_TOPIC, KAFKA_BOOTSTRAP_SERVERS)

    try:
        while running:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
            if not messages:
                continue
            ids, rows = parse(messages)
            results = score(preprocessor, scorer, ids, rows) if rows else []
            for result in results:
                producer.produce(SCORES_TOPIC, key=result['transaction_id'], value=json.dumps(result))
            producer.flush()
            # Оффсеты фиксируем только после того, как результаты ушли в Kafka
            consumer.commit(asynchronous=False)
            n_fraud = sum(r['fraud_flag'] for r in results)
            logger.info('Scored %d of %d messages, fraud: %d', len(results), len(messages), n_fraud)
    finally:
        consumer.close()
        producer.flush()


if __name__ == '__main__':
    main()
