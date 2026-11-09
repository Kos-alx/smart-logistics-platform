-- Числовое состояние потребителей журналов: Core → PG Mart и PG Mart → CH.
CREATE TABLE IF NOT EXISTS meta_data.mart_checkpoint (
    pipeline_name TEXT PRIMARY KEY,
    last_change_id BIGINT NOT NULL CHECK (last_change_id >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
