
-- Аналитический слой PostgreSQL.
CREATE SCHEMA IF NOT EXISTS mart;

-- Подготовленные данные о ценах для аналитических витрин.
-- Гранулярность: цена × месяц × группа кузова.
-- Таблица создаётся пустой, наполнение выполняет Airflow.

CREATE TABLE IF NOT EXISTS mart.prices_prepared (
    price_id             BIGINT NOT NULL,
    month                DATE NOT NULL,
    -- Первая дата цены внутри месяца для отбора истории на дату среза.
    first_price_date     DATE NOT NULL,

    loading_date         DATE NOT NULL,
    loading_city_gcid    INTEGER NOT NULL,
    unloading_city_gcid  INTEGER NOT NULL,
    origin_city          VARCHAR,
    destination_city     VARCHAR,

    distance             DOUBLE PRECISION NOT NULL,
    total_weight         DOUBLE PRECISION NOT NULL,
    total_volume         DOUBLE PRECISION NOT NULL,
    weight_category      VARCHAR(4),
    volume_category      VARCHAR(4),

    body_group_id        INTEGER NOT NULL,
    body_group_name      TEXT NOT NULL,

    price                DOUBLE PRECISION NOT NULL,
    currency_code        VARCHAR(3) NOT NULL,
    exchange_rate        NUMERIC(18, 8) NOT NULL,
    price_rub            NUMERIC(18, 2) NOT NULL,
    price_km_rub         DOUBLE PRECISION,

    organization_id      INTEGER,
    subscriber_id        INTEGER,
    price_type           SMALLINT NOT NULL,
    is_fact              BOOLEAN NOT NULL,
    complex              BOOLEAN NOT NULL,
    outlier_mark         BOOLEAN NOT NULL,

    created_at           TIMESTAMP(0),
    updated_at           TIMESTAMP(0),
    processed_at         TIMESTAMPTZ NOT NULL,

    CONSTRAINT prices_prepared_pk
        PRIMARY KEY (price_id, month, body_group_id),

    CONSTRAINT prices_prepared_price_positive
        CHECK (price_rub > 0),

    CONSTRAINT prices_prepared_distance_positive
        CHECK (distance > 0),

    CONSTRAINT prices_prepared_first_date_month
        CHECK (DATE_TRUNC('month', first_price_date)::date = month)
);

-- Индекс для построения карточки маршрута.
CREATE INDEX IF NOT EXISTS idx_prices_prepared_month_route
ON mart.prices_prepared (
    month,
    loading_city_gcid,
    unloading_city_gcid,
    body_group_id,
    weight_category
);

-- Индекс для отбора наблюдений на дату среза.
CREATE INDEX IF NOT EXISTS idx_prices_prepared_first_price_date
ON mart.prices_prepared (first_price_date);
