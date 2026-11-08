-- DQ: карантин записей prices.
-- Одна запись на комбинацию price_id + reason_code.
-- Исправленные ошибки не удаляем, а помечаем как resolved.

CREATE TABLE IF NOT EXISTS dq.quarantine_prices (
    price_id     BIGINT NOT NULL,
    loading_date DATE NOT NULL,
    reason_code  VARCHAR(100) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    is_resolved  BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at  TIMESTAMPTZ,

    CONSTRAINT quarantine_prices_pkey
        PRIMARY KEY (price_id, reason_code),

    CONSTRAINT quarantine_prices_reason_check
        CHECK (
            reason_code IN (
                'INVALID_PRICE',
                'LOW_RUB_PRICE',
                'INVALID_DISTANCE',
                'NEGATIVE_WEIGHT',
                'NEGATIVE_VOLUME',
                'MISSING_LOADING_CITY',
                'MISSING_UNLOADING_CITY',
                'MISSING_RATE_DATE',
                'MISSING_EXCHANGE_RATE',
                'UNSUPPORTED_CURRENCY'
            )
        ),

    CONSTRAINT quarantine_prices_resolution_check
        CHECK (
            (is_resolved = FALSE AND resolved_at IS NULL)
            OR
            (is_resolved = TRUE AND resolved_at IS NOT NULL)
        )
);

-- Ускоряет получение статистики по активным ошибкам.
CREATE INDEX IF NOT EXISTS idx_quarantine_prices_active
    ON dq.quarantine_prices (reason_code)
    WHERE is_resolved = FALSE;
