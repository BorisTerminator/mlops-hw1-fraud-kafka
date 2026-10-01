-- Витрина скоров. Скрипт выполняется один раз, при первом создании тома с данными Postgres.
CREATE TABLE IF NOT EXISTS fraud_scores (
    id             BIGSERIAL        PRIMARY KEY,
    transaction_id TEXT             NOT NULL UNIQUE,
    score          DOUBLE PRECISION NOT NULL,
    fraud_flag     SMALLINT         NOT NULL CHECK (fraud_flag IN (0, 1)),
    created_at     TIMESTAMPTZ      NOT NULL DEFAULT now()
);

-- Для запроса «10 последних фродовых транзакций»
CREATE INDEX IF NOT EXISTS fraud_scores_fraud_idx ON fraud_scores (id DESC) WHERE fraud_flag = 1;
