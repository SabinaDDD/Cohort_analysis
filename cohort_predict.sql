
WITH events_data AS (
SELECT
    user_id,                                     --   юзер
    toStartOfWeek(toDate(event_datetime), 1) as __week,    -- неделя
    event_name as __event_name                          -- событие
FROM sdautova.toy_events_sdautova
)
, all_with_first_event AS (
    SELECT * FROM events_data         -- юзеры с первым событием
    WHERE user_id in (SELECT user_id FROM events_data WHERE __event_name = '{{ var('event_start')}}') -- юзеры, у которых есть первое событие
)
, users_with_dates AS (
    SELECT
        user_id,               -- юзеры с первой и последней датой 
        --maxIf(__week, __event_name = '{{ var('event_return')}}') as last_week,
        max(__week) as last_week,
        minIf(__week, __event_name = '{{ var('event_start')}}') AS first_week
    FROM all_with_first_event
    GROUP BY user_id
)
, date_matrix AS (
    WITH
        (SELECT min(__week) FROM events_data) AS min_week,
        (SELECT max(__week) FROM events_data) AS max_week,
        (SELECT dateDiff('week', min_week, max_week)) AS week_range -- разница в днях между первым и последним днем
    SELECT
        week_start,
        delta
    FROM
        (
            SELECT
                week_start,
                number AS delta
            FROM
                (
                    SELECT arrayJoin(range(week_range + 1)) AS number
                ) AS numbers
            CROSS JOIN
                (SELECT DISTINCT __week AS week_start FROM events_data) AS weeks
        )
    ORDER BY week_start, delta
)

, delta_active_users_0 as (
    
    SELECT
        date_matrix.week_start as week_start,
        date_matrix.delta as delta,
        countIf(date_matrix.week_start + INTERVAL delta WEEK <= users_with_dates.last_week) AS active_users
    FROM date_matrix
    LEFT JOIN users_with_dates ON date_matrix.week_start = users_with_dates.first_week
    GROUP BY date_matrix.week_start, date_matrix.delta
    ORDER BY date_matrix.week_start, date_matrix.delta
), 
{% set ns = namespace(k = 0) %}  ------------задаем начальное значение для счетчика кол-ва циклов 
{% set start_date_str = var('start_date') %}------------начальная дата из Var
{% set delt_cnt = var('delt_cnt')|int %}----------------кол-ва дельт полученное из Var
{% set week_starts = generate_dates(start_date_str) %}
-------------------------внешний цикл по delta------------------------------------------------------------------
{% for i in range(1, delt_cnt) %} --------------  сюда нужно передать переменную вместо числа 9 - -кол-во дельт
------------------------внутренний цикл по когортам -------------------------------------------------------------------------
{% for j in range(week_starts|length) %}
{% if i + j > 8  %}---------определяем для каких именно ячеек рассчитывать предикт -----------------
{% set ns.k = ns.k + 1 %}----------------------------наращиваем счетчик для нумерации СТЕшек-------------------
---------------------------СТЕ с рассчетом числа активных пользователей для текущей ячейки ----------------------------------
delta_active_users_{{ns.k|int}} as (
    SELECT week_start, toInt32(delta) as delta, toInt32(active_users) as active_users
    FROM delta_active_users_{{ns.k-1|int}}
    WHERE NOT (week_start = toDate('{{ week_starts[j] }}') and delta = {{i}})
    union all 
    SELECT toDate('{{ week_starts[j] }}') as week_start, toInt32({{i}}) as delta, toInt32(ROUND(predicted_value)) as active_users
    FROM 
    ( ---------------------------------------подзапрос c predicted value -------------------------------------------
        SELECT (SUM(CASE WHEN delta = {{i}} AND week_start < toDate('{{ week_starts[j] }}') THEN active_users END)/
            SUM(CASE WHEN delta = {{i-1}} AND week_start < toDate('{{ week_starts[j] }}') THEN active_users END))*
            SUM(CASE WHEN delta = {{i-1}} AND week_start = toDate('{{ week_starts[j] }}') THEN active_users END) as predicted_value
        FROM delta_active_users_{{ns.k-1|int}}
    )-----------------------------------конец подзапрос PREDICT------------------------------------------   
)----------------------------------конец СТЕ delta_active_users
{% if not (j == week_starts|length -1  and i == delt_cnt-1) %}, {% endif %}-----ставим запятую между СТЕшками
{% endif %}
{%- endfor -%}-------------------------конец внутреннего цикла ----------------------------------
{%- endfor -%}-------------------------конец внешнего цикла -------------------------------------
SELECT * FROM delta_active_users_{{ns.k|int}}