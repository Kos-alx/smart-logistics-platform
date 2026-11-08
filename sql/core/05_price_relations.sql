-- Core: дочерние таблицы prices.
-- Физические FOREIGN KEY не используем.
-- Ссылочную целостность контролирует ETL.

-- Даты сделок
CREATE TABLE IF NOT EXISTS core.price_dates (
    id         BIGINT PRIMARY KEY,
    price_id   BIGINT NOT NULL,
    price_date DATE NOT NULL,

    CONSTRAINT core_price_dates_unique
        UNIQUE (price_id, price_date)
);


-- Типы кузовов сделок
CREATE TABLE IF NOT EXISTS core.price_body_types (
    id           BIGINT PRIMARY KEY,
    price_id     BIGINT NOT NULL,
    body_type_id INTEGER NOT NULL,

    CONSTRAINT core_price_body_types_unique
        UNIQUE (price_id, body_type_id)
);


-- Участники сделок
CREATE TABLE IF NOT EXISTS core.price_participants (
    id            BIGINT PRIMARY KEY,
    price_id      BIGINT NOT NULL,
    subscriber_id BIGINT NOT NULL,

    CONSTRAINT core_price_participants_unique
        UNIQUE (price_id, subscriber_id)
);
