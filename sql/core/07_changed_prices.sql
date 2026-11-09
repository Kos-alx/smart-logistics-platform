-- Журнал изменений Core для инкрементального обновления Mart.
-- Каждое событие содержит идентификатор цены и источник изменения.

CREATE TABLE IF NOT EXISTS meta_data.changed_prices (
    change_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    price_id BIGINT NOT NULL,
    source_table TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT changed_prices_source_check
        CHECK (
            source_table IN (
                'prices',
                'price_dates',
                'price_body_types'
            )
        )
);

-- Индекс по price_id для поиска истории изменений.
CREATE INDEX IF NOT EXISTS idx_changed_prices_price_id
    ON meta_data.changed_prices (price_id);
