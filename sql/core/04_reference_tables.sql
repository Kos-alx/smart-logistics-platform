
CREATE TABLE IF NOT EXISTS core.cities (
    id              BIGINT NOT NULL,
    full_name_local VARCHAR NOT NULL,
    full_name_gc    VARCHAR NOT NULL,
    gc_id           INTEGER PRIMARY KEY,
    name            VARCHAR NOT NULL,
    longitude       DOUBLE PRECISION,
    latitude        DOUBLE PRECISION,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,

    CONSTRAINT core_cities_id_unique UNIQUE (id)
);

CREATE TABLE IF NOT EXISTS core.exchange_rates (
    id           BIGINT PRIMARY KEY,
    num_code     VARCHAR(255) NOT NULL,
    value        DOUBLE PRECISION NOT NULL,
    setting_date DATE NOT NULL,

    CONSTRAINT core_exchange_rates_unique
        UNIQUE (num_code, setting_date),

    CONSTRAINT core_exchange_rates_value_positive
        CHECK (value > 0)
);

CREATE TABLE IF NOT EXISTS core.body_types (
    id         BIGINT PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    id_from_1C INTEGER NOT NULL,
    std_id     INTEGER,

    CONSTRAINT core_body_types_std_id_fkey
        FOREIGN KEY (std_id)
        REFERENCES core.body_types(id)
);
