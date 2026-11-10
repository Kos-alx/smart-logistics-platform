-- ML: одна строка на сделку; только Core, без ценовых агрегатов.
WITH effective_body_types AS (
    SELECT price_id, body_type_id
    FROM core.price_body_types

    UNION ALL

    SELECT p.id, p.body_type
    FROM core.prices AS p
    WHERE NOT EXISTS (
        SELECT 1 FROM core.price_body_types AS pbt WHERE pbt.price_id = p.id
    )
), mapped_body_groups AS (
    SELECT
        e.price_id,
        CASE
            WHEN e.body_type_id IS NULL THEN -2
            WHEN COALESCE(bt.std_id, e.body_type_id) IN (56, 4, 2, 15)
                THEN COALESCE(bt.std_id, e.body_type_id)
            ELSE -1
        END AS body_group_id
    FROM effective_body_types AS e
    LEFT JOIN core.body_types AS bt ON bt.id = e.body_type_id
), body_groups AS (
    SELECT
        price_id,
        ARRAY_AGG(DISTINCT body_group_id ORDER BY body_group_id) AS body_group_ids
    FROM mapped_body_groups
    GROUP BY price_id
)
SELECT
    p.id AS price_id,
    p.loading_date,
    p.loading_city_gcid,
    p.unloading_city_gcid,
    origin.full_name_local AS origin_full_name,
    COALESCE(NULLIF(BTRIM(SPLIT_PART(origin.full_name_local, ',', 1)), ''),
             origin.name) AS origin_city,
    CASE WHEN STRPOS(origin.full_name_local, ',') > 0
         THEN NULLIF(BTRIM(SUBSTRING(origin.full_name_local
                                    FROM STRPOS(origin.full_name_local, ',') + 1)), '')
    END AS origin_region,
    origin.latitude AS origin_latitude,
    origin.longitude AS origin_longitude,
    destination.full_name_local AS destination_full_name,
    COALESCE(NULLIF(BTRIM(SPLIT_PART(destination.full_name_local, ',', 1)), ''),
             destination.name) AS destination_city,
    CASE WHEN STRPOS(destination.full_name_local, ',') > 0
         THEN NULLIF(BTRIM(SUBSTRING(destination.full_name_local
                                    FROM STRPOS(destination.full_name_local, ',') + 1)), '')
    END AS destination_region,
    destination.latitude AS destination_latitude,
    destination.longitude AS destination_longitude,
    p.distance,
    p.total_weight,
    p.total_volume,
    p.weight_category,
    p.volume_category,
    p.body_type AS source_body_type_id,
    bg.body_group_ids,
    p.price_rub,
    p.is_fact,
    p.price_type,
    p.complex,
    p.outlier_mark
FROM core.prices AS p
LEFT JOIN core.cities AS origin ON origin.gc_id = p.loading_city_gcid
LEFT JOIN core.cities AS destination ON destination.gc_id = p.unloading_city_gcid
LEFT JOIN body_groups AS bg ON bg.price_id = p.id
ORDER BY p.id;
