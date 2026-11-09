-- Карточка маршрута на фиксированную дату снимка задания.
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mart_route_card AS
-- Начало расчёта
WITH monthly AS (
    SELECT
        month, loading_city_gcid, unloading_city_gcid, body_group_id, weight_category,
        MIN(origin_city) AS origin_city,
        MIN(destination_city) AS destination_city,
        MIN(body_group_name) AS body_group_name,
        PERCENTILE_CONT(ARRAY[0.25, 0.5, 0.75])
            WITHIN GROUP (ORDER BY price_rub::double precision) AS trip_quantiles,
        PERCENTILE_CONT(ARRAY[0.25, 0.5, 0.75])
            WITHIN GROUP (ORDER BY price_rub::double precision / NULLIF(distance, 0)) AS km_quantiles,
        AVG(price_rub)::double precision AS avg_trip,
        COUNT(DISTINCT price_id) AS trips_count,
        COUNT(DISTINCT organization_id) FILTER (WHERE price_type IN (3, 9)) AS carriers_count,
        STDDEV_SAMP(price_rub)::double precision
            / NULLIF(AVG(price_rub)::double precision, 0) AS cv_rate
    FROM mart.prices_prepared
    WHERE is_fact = TRUE
      AND price_type IN (1, 2, 3, 4, 5, 9)
      AND complex = FALSE
      AND outlier_mark = FALSE
      AND price > 0 AND distance > 0 AND price_rub > 0
      AND first_price_date <= DATE '2026-10-26'
    GROUP BY month, loading_city_gcid, unloading_city_gcid, body_group_id, weight_category
), compared AS (
    SELECT *,
        -- Точные календарные смещения, а не предыдущая доступная строка.
        MAX(trip_quantiles[2]) OVER (calendar RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING)
            AS previous_month_median,
        MAX(trip_quantiles[2]) OVER (calendar RANGE BETWEEN 12 PRECEDING AND 12 PRECEDING)
            AS previous_year_median,
        DATE '2026-10-26' - LEAST(
            DATE '2026-10-26', (month + INTERVAL '1 month' - INTERVAL '1 day')::date
        ) AS freshness_days
    FROM monthly
    WINDOW calendar AS (
        PARTITION BY loading_city_gcid, unloading_city_gcid, body_group_id, weight_category
        ORDER BY (EXTRACT(YEAR FROM month)::int * 12 + EXTRACT(MONTH FROM month)::int)
    )
)
SELECT
    month, loading_city_gcid, unloading_city_gcid, body_group_id, weight_category,
    origin_city, destination_city, body_group_name,
    DATE '2026-10-26' AS as_of,
    trip_quantiles[1] AS p25_trip,
    trip_quantiles[2] AS median_trip,
    trip_quantiles[3] AS p75_trip,
    avg_trip,
    km_quantiles[1] AS p25_km,
    km_quantiles[2] AS median_km,
    km_quantiles[3] AS p75_km,
    trips_count, carriers_count, cv_rate,
    100.0 * (trip_quantiles[2] - previous_month_median)
        / NULLIF(previous_month_median, 0) AS mom_pct,
    100.0 * (trip_quantiles[2] - previous_year_median)
        / NULLIF(previous_year_median, 0) AS yoy_pct,
    freshness_days,
    CASE
        WHEN trips_count >= 30 AND carriers_count >= 5 AND freshness_days <= 30 THEN 'HIGH'
        WHEN trips_count >= 10 THEN 'MEDIUM'
        ELSE 'LOW_DATA'
    END AS reliability_level
FROM compared
-- Конец расчёта
WITH NO DATA;

-- NULL весовой категории также образует единственную аналитическую ячейку.
CREATE UNIQUE INDEX IF NOT EXISTS idx_route_card_grain
ON mart.mart_route_card (
    month, loading_city_gcid, unloading_city_gcid, body_group_id, weight_category
) NULLS NOT DISTINCT;
