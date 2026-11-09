-- Подготовленный Mart: логическая копия PostgreSQL с месячными партициями.
CREATE TABLE IF NOT EXISTS prices_prepared (
    price_id Int64,
    month Date,
    first_price_date Date,
    loading_date Date,
    loading_city_gcid Int32,
    unloading_city_gcid Int32,
    origin_city Nullable(String),
    destination_city Nullable(String),
    distance Float64,
    total_weight Float64,
    total_volume Float64,
    weight_category Nullable(String),
    volume_category Nullable(String),
    body_group_id Int32,
    body_group_name LowCardinality(String),
    price Float64,
    currency_code LowCardinality(String),
    exchange_rate Decimal(18, 8),
    price_rub Decimal(18, 2),
    price_km_rub Nullable(Float64),
    organization_id Nullable(Int32),
    subscriber_id Nullable(Int32),
    price_type Int16,
    is_fact Bool,
    complex Bool,
    outlier_mark Bool,
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    processed_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (
    month, loading_city_gcid, unloading_city_gcid, body_group_id,
    ifNull(weight_category, ''), price_id
);

-- Служебная таблица с теми же колонками, ключами и партиционированием.
CREATE TABLE IF NOT EXISTS prices_prepared_stage AS prices_prepared;
