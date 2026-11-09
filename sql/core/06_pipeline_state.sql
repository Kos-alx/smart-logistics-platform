
CREATE TABLE IF NOT EXISTS meta_data.pipeline_state (
    pipeline_name TEXT PRIMARY KEY,
    watermark TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
