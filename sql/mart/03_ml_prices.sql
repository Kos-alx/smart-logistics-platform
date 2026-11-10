-- Реестр моделей; двоичный артефакт хранится в общем каталоге Airflow.
CREATE TABLE IF NOT EXISTS mart.ml_models (
    model_version TEXT PRIMARY KEY CHECK (model_version ~ '^[0-9a-f]{64}$'),
    model_name TEXT NOT NULL,
    as_of DATE NOT NULL,
    artifact_path TEXT NOT NULL,
    model_params JSONB NOT NULL,
    feature_columns JSONB NOT NULL,
    metrics JSONB NOT NULL,
    manifest JSONB NOT NULL,
    trained_at TIMESTAMPTZ NOT NULL
);

-- Одна строка на сделку и версию; группы кузова не размножают прогноз.
CREATE TABLE IF NOT EXISTS mart.ml_price_predictions (
    price_id BIGINT NOT NULL,
    model_version TEXT NOT NULL REFERENCES mart.ml_models(model_version),
    as_of DATE NOT NULL,
    loading_date DATE NOT NULL,
    loading_city_gcid INTEGER NOT NULL,
    unloading_city_gcid INTEGER NOT NULL,
    body_group_ids INTEGER[] NOT NULL,
    prediction_set TEXT NOT NULL CHECK (prediction_set IN ('test', 'future')),
    actual_price_rub DOUBLE PRECISION,
    predicted_price_rub DOUBLE PRECISION NOT NULL,
    abs_error_rub DOUBLE PRECISION,
    ape_pct DOUBLE PRECISION,
    model_name TEXT NOT NULL,
    scored_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (price_id, model_version, as_of),
    CHECK (
        (prediction_set = 'future' AND loading_date > as_of
         AND actual_price_rub IS NULL AND abs_error_rub IS NULL AND ape_pct IS NULL)
        OR
        (prediction_set = 'test' AND loading_date <= as_of
         AND actual_price_rub > 0 AND abs_error_rub >= 0 AND ape_pct >= 0
         AND actual_price_rub IS NOT NULL AND abs_error_rub IS NOT NULL AND ape_pct IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_loading_date
    ON mart.ml_price_predictions (loading_date);
