CREATE TABLE IF NOT EXISTS raw_train_movements (
    id SERIAL PRIMARY KEY,
    train_id TEXT,
    loc_stanox TEXT,
    tiploc TEXT,
    alpha3 TEXT,
    local_name TEXT,
    event_type TEXT,
    planned_timestamp BIGINT,
    actual_timestamp BIGINT,
    variation_status TEXT,
    delay_seconds DOUBLE PRECISION,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_train_cancellations (
    id SERIAL PRIMARY KEY,
    train_id TEXT,
    train_service_code TEXT,
    toc_id TEXT,
    loc_stanox TEXT,
    tiploc TEXT,
    alpha3 TEXT,
    local_name TEXT,
    canx_timestamp BIGINT,
    canx_reason_code TEXT,
    canx_type TEXT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);