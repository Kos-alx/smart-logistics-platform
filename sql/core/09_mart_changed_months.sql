-- Журнал месяцев подготовленного Mart для синхронизации с ClickHouse.
-- Старые и новые месяцы затронутых цен записываются в транзакции обновления Mart.
-- Повторения месяца между запусками допустимы: потребитель выбирает уникальные
-- месяцы после своего checkpoint. Сам журнал не помечает события обработанными.
CREATE TABLE IF NOT EXISTS meta_data.mart_changed_months (
    change_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    month DATE NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT mart_changed_months_month_start
        CHECK (month = DATE_TRUNC('month', month)::date)
);
