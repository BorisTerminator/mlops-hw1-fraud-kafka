import json
import os
import uuid

import altair as alt
import pandas as pd
import psycopg
import streamlit as st
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TRANSACTIONS_TOPIC = os.getenv('KAFKA_TRANSACTIONS_TOPIC', 'transactions')
DSN = (
    f"host={os.getenv('POSTGRES_HOST', 'postgres')} "
    f"port={os.getenv('POSTGRES_PORT', '5432')} "
    f"dbname={os.getenv('POSTGRES_DB', 'fraud')} "
    f"user={os.getenv('POSTGRES_USER', 'fraud')} "
    f"password={os.getenv('POSTGRES_PASSWORD', 'fraud')}"
)

LAST_FRAUD_SQL = """
    SELECT transaction_id, score, fraud_flag, created_at
    FROM fraud_scores
    WHERE fraud_flag = 1
    ORDER BY id DESC
    LIMIT 10
"""
LAST_SCORES_SQL = "SELECT score FROM fraud_scores ORDER BY id DESC LIMIT 100"


def send_to_kafka(df):
    """Каждая строка CSV уходит в Kafka отдельным сообщением со своим transaction_id."""
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    # NaN не сериализуется в валидный JSON — заменяем на None (null)
    records = df.astype(object).where(df.notna(), None).to_dict(orient='records')
    progress = st.progress(0.0)
    for i, row in enumerate(records, start=1):
        tid = str(uuid.uuid4())
        producer.produce(TRANSACTIONS_TOPIC, key=tid,
                         value=json.dumps({'transaction_id': tid, 'data': row}, default=str))
        producer.poll(0)
        if i % 100 == 0 or i == len(records):
            progress.progress(i / len(records))
    undelivered = producer.flush(30)
    if undelivered:
        raise RuntimeError(f'{undelivered} messages were not delivered')


def query(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])


def fetch_results():
    with psycopg.connect(DSN) as conn:
        return query(conn, LAST_FRAUD_SQL), query(conn, LAST_SCORES_SQL)


def page_send():
    st.title('📤 Отправка транзакций в Kafka')
    st.caption(f'Топик `{TRANSACTIONS_TOPIC}`, брокер `{KAFKA_BOOTSTRAP_SERVERS}`')

    uploaded = st.file_uploader('CSV-файл с транзакциями в формате test.csv', type=['csv'])
    if uploaded is None:
        return

    df = pd.read_csv(uploaded)
    st.write(f'Строк в файле: **{len(df)}**')
    st.dataframe(df.head(), use_container_width=True)

    if st.button(f'Отправить {len(df)} транзакций', type='primary'):
        try:
            with st.spinner('Отправка...'):
                send_to_kafka(df)
            st.success(f'Отправлено {len(df)} сообщений в топик `{TRANSACTIONS_TOPIC}`')
        except Exception as e:
            st.error(f'Ошибка отправки: {e}')


def page_results():
    st.title('📊 Результаты скоринга')

    if st.button('Посмотреть результаты', type='primary'):
        try:
            st.session_state.results = fetch_results()
        except Exception as e:
            st.error(f'Не удалось получить данные из Postgres: {e}')

    if 'results' not in st.session_state:
        return
    fraud, scores = st.session_state.results

    st.subheader('10 последних транзакций с fraud_flag = 1')
    if fraud.empty:
        st.info('Фродовых транзакций в базе пока нет')
    else:
        st.dataframe(fraud, use_container_width=True, hide_index=True)

    st.subheader(f'Распределение скоров последних {len(scores)} транзакций')
    if scores.empty:
        st.info('В базе пока нет ни одной транзакции')
    else:
        chart = alt.Chart(scores).mark_bar().encode(
            x=alt.X('score:Q', bin=alt.Bin(extent=[0, 1], step=0.05), title='Скор модели'),
            y=alt.Y('count():Q', title='Число транзакций'),
        )
        st.altair_chart(chart, use_container_width=True)


st.set_page_config(page_title='Fraud scoring', page_icon='🕵️')
page = st.sidebar.radio('Раздел', ['Отправка транзакций', 'Результаты'])
if page == 'Отправка транзакций':
    page_send()
else:
    page_results()
