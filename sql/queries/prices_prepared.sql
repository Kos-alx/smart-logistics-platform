-- Подготовленные данные: одна строка на цену, месяц и бизнес-группу кузова.
-- Сохраняем все месяцы и исходные флаги. Ограничение по as_of и фильтры
-- рыночной выборки применяются при построении карточки маршрута.
-- Запрос только читает Core; таблицы источника public не используются.
WITH price_months AS (
    SELECT
        d.price_id,
        DATE_TRUNC('month', d.price_date)::date AS month,
        MIN(d.price_date) AS first_price_date
    FROM core.price_dates AS d
    JOIN core.prices AS p ON p.id = d.price_id
    GROUP BY d.price_id, DATE_TRUNC('month', d.price_date)::date
),
effective_body_types AS (
    SELECT pbt.price_id, pbt.body_type_id
    FROM core.price_body_types AS pbt
    JOIN core.prices AS p ON p.id = pbt.price_id

    UNION ALL

    -- При отсутствии связей используем кузов непосредственно из цены.
    SELECT p.id, p.body_type
    FROM core.prices AS p
    WHERE NOT EXISTS (
        SELECT 1
        FROM core.price_body_types AS pbt
        WHERE pbt.price_id = p.id
    )
),
mapped_body_groups AS (
    SELECT
        e.price_id,
        CASE
            WHEN e.body_type_id IS NULL THEN -2
            WHEN COALESCE(bt.std_id, e.body_type_id) = 56 THEN 56
            WHEN COALESCE(bt.std_id, e.body_type_id) = 4 THEN 4
            WHEN COALESCE(bt.std_id, e.body_type_id) = 2 THEN 2
            WHEN COALESCE(bt.std_id, e.body_type_id) = 15 THEN 15
            ELSE -1
        END AS body_group_id
    FROM effective_body_types AS e
    LEFT JOIN core.body_types AS bt ON bt.id = e.body_type_id
),
price_body_groups AS (
    -- Несколько дочерних кузовов одной группы не размножают цену.
    SELECT DISTINCT price_id, body_group_id
    FROM mapped_body_groups
)
SELECT
    p.id AS price_id,
    pm.month,
    pm.first_price_date,
    p.loading_date,
    p.loading_city_gcid,
    p.unloading_city_gcid,
    origin.name AS origin_city,
    destination.name AS destination_city,
    p.distance,
    p.total_weight,
    p.total_volume,
    p.weight_category,
    p.volume_category,
    bg.body_group_id,
    CASE bg.body_group_id
        WHEN 56 THEN 'Тент'
        WHEN 4 THEN 'Рефрижератор'
        WHEN 2 THEN 'Изотерм'
        WHEN 15 THEN 'Борт'
        WHEN -1 THEN 'Прочие'
        WHEN -2 THEN 'Не указан'
    END AS body_group_name,
    p.price,
    p.currency_code,
    p.exchange_rate,
    p.price_rub,
    p.price_rub::double precision / NULLIF(p.distance, 0) AS price_km_rub,
    p.organization_id,
    p.subscriber_id,
    p.price_type,
    p.is_fact,
    p.complex,
    p.outlier_mark,
    p.created_at,
    p.updated_at,
    CURRENT_TIMESTAMP AS processed_at
FROM core.prices AS p
JOIN price_months AS pm ON pm.price_id = p.id
JOIN price_body_groups AS bg ON bg.price_id = p.id
JOIN core.cities AS origin ON origin.gc_id = p.loading_city_gcid
JOIN core.cities AS destination ON destination.gc_id = p.unloading_city_gcid;
