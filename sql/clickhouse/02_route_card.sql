-- Готовый результат обновляется DAG. Периодические обновления остановлены.
CREATE MATERIALIZED VIEW IF NOT EXISTS mart_route_card
REFRESH AFTER 1 YEAR
ENGINE = MergeTree
ORDER BY (month, loading_city_gcid, unloading_city_gcid, body_group_id, ifNull(weight_category, ''))
EMPTY AS
-- Начало расчёта
WITH monthly AS (
    SELECT
        month, loading_city_gcid, unloading_city_gcid, body_group_id, weight_category,
        min(origin_city) AS origin_city,
        min(destination_city) AS destination_city,
        min(body_group_name) AS body_group_name,
        quantilesExactInclusive(0.25, 0.5, 0.75)(toFloat64(price_rub)) AS trip_quantiles,
        quantilesExactInclusive(0.25, 0.5, 0.75)(
            toFloat64(price_rub) / nullIf(distance, 0)) AS km_quantiles,
        avg(toFloat64(price_rub)) AS avg_trip,
        uniqExact(price_id) AS trips_count,
        uniqExactIf(organization_id, price_type IN (3, 9)) AS carriers_count,
        if(count() < 2, NULL,
            stddevSampStable(toFloat64(price_rub)) / nullIf(avg(toFloat64(price_rub)), 0)) AS cv_rate
    FROM prices_prepared
    WHERE is_fact = true
      AND price_type IN (1, 2, 3, 4, 5, 9)
      AND complex = false
      AND outlier_mark = false
      AND price > 0 AND distance > 0 AND price_rub > 0
      AND first_price_date <= toDate('2026-10-26')
    GROUP BY month, loading_city_gcid, unloading_city_gcid, body_group_id, weight_category
), compared AS (
    SELECT *,
        -- Пустое календарное окно даёт NULL даже при наличии более старых месяцев.
        maxOrNull(trip_quantiles[2]) OVER (calendar RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING)
            AS previous_month_median,
        maxOrNull(trip_quantiles[2]) OVER (calendar RANGE BETWEEN 12 PRECEDING AND 12 PRECEDING)
            AS previous_year_median,
        dateDiff('day', least(toDate('2026-10-26'), toLastDayOfMonth(month)),
            toDate('2026-10-26')) AS freshness_days
    FROM monthly
    WINDOW calendar AS (
        PARTITION BY loading_city_gcid, unloading_city_gcid, body_group_id, weight_category
        ORDER BY toRelativeMonthNum(month)
    )
)
SELECT
    month, loading_city_gcid, unloading_city_gcid, body_group_id, weight_category,
    origin_city, destination_city, body_group_name,
    toDate('2026-10-26') AS as_of,
    trip_quantiles[1] AS p25_trip,
    trip_quantiles[2] AS median_trip,
    trip_quantiles[3] AS p75_trip,
    avg_trip,
    km_quantiles[1] AS p25_km,
    km_quantiles[2] AS median_km,
    km_quantiles[3] AS p75_km,
    trips_count, carriers_count, cv_rate,
    100.0 * (trip_quantiles[2] - previous_month_median)
        / nullIf(previous_month_median, 0) AS mom_pct,
    100.0 * (trip_quantiles[2] - previous_year_median)
        / nullIf(previous_year_median, 0) AS yoy_pct,
    freshness_days,
    multiIf(trips_count >= 30 AND carriers_count >= 5 AND freshness_days <= 30, 'HIGH',
        trips_count >= 10, 'MEDIUM', 'LOW_DATA') AS reliability_level
FROM compared
-- Конец расчёта
;

SYSTEM STOP VIEW mart_route_card;
