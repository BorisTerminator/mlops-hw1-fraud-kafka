# Real-time fraud scoring на Kafka

Сервис скоринга фродовых транзакций в потоке. Транзакции в формате `test.csv` соревнования
[teta-ml-1-2025](https://www.kaggle.com/competitions/teta-ml-1-2025) приходят в топик Kafka,
модель CatBoost считает вероятность фрода, результат уходит в другой топик и складывается в Postgres.
Inference только на CPU, обучения в сервисе нет.

## Архитектура

```
 Streamlit UI ──► [transactions] ──► fraud_detector ──► [scores] ──► score_writer ──► Postgres
 (отправка CSV)       Kafka          препроцессинг +      Kafka                     fraud_scores
                                     модель CatBoost                                     │
 Streamlit UI ◄──────────────────────── «Посмотреть результаты» ◄────────────────────────┘
```

| Сервис | Что делает |
|---|---|
| `zookeeper`, `kafka` | брокер сообщений |
| `kafka-setup` | создаёт топики `transactions` и `scores` и завершается |
| `kafka-ui` | веб-интерфейс Kafka, http://localhost:8080 |
| `fraud_detector` | читает `transactions`, делает препроцессинг и скоринг, пишет в `scores` |
| `postgres` | БД с витриной `fraud_scores` |
| `score_writer` | читает `scores` и пишет в `fraud_scores` |
| `interface` | Streamlit, http://localhost:8501: отправка CSV и просмотр результатов |

`fraud_detector` разбит на три скрипта по этапам:

- [`src/service.py`](fraud_detector/src/service.py) — чтение сообщений из Kafka и запись результата в Kafka;
- [`src/preprocessing.py`](fraud_detector/src/preprocessing.py) — препроцессинг по логике из соревнования: коды категорий по частоте, временные признаки, mean target encoding, расстояние клиент–мерчант, заполнение пропусков, логарифм;
- [`src/scorer.py`](fraud_detector/src/scorer.py) — скоринг моделью `models/my_catboost.cbm`, флаг фрода при `score > 0.98`.

Статистики по train, нужные препроцессингу, посчитаны заранее скриптом
[`tools/build_artifacts.py`](tools/build_artifacts.py) и лежат в `fraud_detector/artifacts/preprocessing.json` (83 КБ).
Поэтому `train.csv` (138 МБ) не нужен ни в образе, ни в репозитории. Пересчитать их можно так:
`python tools/build_artifacts.py --train data/train.csv`.

### Форматы сообщений

Топик `transactions` — одна транзакция в одном сообщении:

```json
{"transaction_id": "6f1c...", "data": {"transaction_time": "2019-09-14 02:46", "merch": "fraud_Stokes, ...", "amount": 25.79, "...": "..."}}
```

Топик `scores`:

```json
{"transaction_id": "6f1c...", "score": 0.0123, "fraud_flag": 0}
```

Витрина в Postgres ([`postgres/init.sql`](postgres/init.sql)):

| колонка | тип |
|---|---|
| `id` | `bigserial`, порядок поступления |
| `transaction_id` | `text`, уникальный |
| `score` | `double precision` |
| `fraud_flag` | `smallint` (0/1) |
| `created_at` | `timestamptz` |

## Запуск

Нужны Docker 20.10+ и Docker Compose v2. На хосте должны быть свободны порты 8080, 8501 и 9095.

```bash
git clone https://github.com/<user>/<repo>.git
cd <repo>
docker compose up -d --build
```

Первая сборка занимает несколько минут. Проверка, что всё поднялось:

```bash
docker compose ps -a
```

Все сервисы в состоянии `running`, кроме `kafka-setup`: он должен быть `Exited (0)`, так и задумано.

## Проверка работы

1. Откройте http://localhost:8501, раздел **«Отправка транзакций»**.
2. Загрузите [`data/sample_test.csv`](data/sample_test.csv) из репозитория: 200 строк формата `test.csv`,
   из них 15 модель помечает как фрод. Подойдёт и любой фрагмент `test.csv` из соревнования.
3. Нажмите **«Отправить 200 транзакций»**.
4. Перейдите в раздел **«Результаты»** и нажмите **«Посмотреть результаты»**. Появятся
   10 последних транзакций с `fraud_flag = 1` и гистограмма скоров последних 100 транзакций.

Что можно посмотреть дополнительно:

```bash
# логи скоринга: "Scored 200 of 200 messages, fraud: 15"
docker compose logs fraud_detector

# логи записи в БД: "Saved 200 scores"
docker compose logs score_writer

# содержимое витрины
docker compose exec postgres psql -U fraud -d fraud -c "select count(*), sum(fraud_flag) from fraud_scores"
```

Сообщения в топиках `transactions` и `scores` видны в Kafka UI: http://localhost:8080 → Topics.

## Остановка

```bash
docker compose down      # остановить, данные в БД сохранятся
docker compose down -v   # остановить и удалить данные БД
```

## Структура проекта

```
.
├── docker-compose.yml
├── fraud_detector/
│   ├── src/service.py          # Kafka consumer/producer
│   ├── src/preprocessing.py    # препроцессинг
│   ├── src/scorer.py           # скоринг моделью
│   ├── models/my_catboost.cbm  # модель
│   ├── artifacts/preprocessing.json
│   ├── requirements.txt
│   └── Dockerfile
├── score_writer/               # scores -> Postgres
├── interface/                  # Streamlit UI
├── postgres/init.sql           # витрина fraud_scores
├── tools/build_artifacts.py    # train.csv -> artifacts/preprocessing.json
└── data/sample_test.csv        # пример для проверки
```

Модель и логика препроцессинга взяты из семинара
[mts25_mlops_hw2_real_time_fraud_detection](https://github.com/NikitaMalykhin/mts25_mlops_hw2_real_time_fraud_detection).
Скоры этого сервиса совпадают с кодом семинара на тех же данных.
