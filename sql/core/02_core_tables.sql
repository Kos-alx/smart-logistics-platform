
CREATE TABLE IF NOT EXISTS core.prices (
    -- Идентификаторы и метаданные источника
    id                  BIGINT PRIMARY KEY,
    created_at          TIMESTAMP(0),
    updated_at          TIMESTAMP(0),
    subscriber_id       INTEGER,
    organization_id     INTEGER,

    -- Маршрут и груз
    loading_date        DATE NOT NULL,
    loading_city_gcid   INTEGER NOT NULL,
    unloading_city_gcid INTEGER NOT NULL,
    distance            DOUBLE PRECISION NOT NULL,
    total_weight        DOUBLE PRECISION NOT NULL,
    total_volume        DOUBLE PRECISION NOT NULL,
    body_type           INTEGER,

    -- Исходная цена
    price               DOUBLE PRECISION NOT NULL,
    currency_code       VARCHAR(3) NOT NULL,
    nearest_rate_date   DATE,

    -- Нормализованная цена
    exchange_rate       NUMERIC(18, 8) NOT NULL,
    price_rub           NUMERIC(18, 2) NOT NULL,

    -- Бизнес-атрибуты
    complex             BOOLEAN NOT NULL,
    price_type          SMALLINT NOT NULL,
    item_id             INTEGER,
    is_fact             BOOLEAN NOT NULL,
    meta_item_id        BIGINT,
    end_loading_date    DATE,
    source              VARCHAR(255),
    external_item_uuid  UUID,
    outlier_mark        BOOLEAN NOT NULL,
    weight_category     VARCHAR(4),
    volume_category     VARCHAR(4),
    price_group_id      BIGINT,

    -- Метаданные ETL
    processed_at        TIMESTAMPTZ NOT NULL
                        DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT core_prices_price_positive
        CHECK (price > 0),

    CONSTRAINT core_prices_rub_minimum
        CHECK (currency_code <> '643' OR price > 1000),

    CONSTRAINT core_prices_distance_positive
        CHECK (distance > 0),

    CONSTRAINT core_prices_weight_nonnegative
        CHECK (total_weight >= 0),

    CONSTRAINT core_prices_volume_nonnegative
        CHECK (total_volume >= 0),

    CONSTRAINT core_prices_currency_supported
        CHECK (currency_code IN ('643', '840', '978')),

    CONSTRAINT core_prices_exchange_rate_positive
        CHECK (exchange_rate > 0),

    CONSTRAINT core_prices_price_rub_positive
        CHECK (price_rub > 0)
);

CREATE INDEX IF NOT EXISTS idx_core_prices_loading_date
    ON core.prices (loading_date);

CREATE INDEX IF NOT EXISTS idx_core_prices_route
    ON core.prices (
        loading_city_gcid,
        unloading_city_gcid
    );
