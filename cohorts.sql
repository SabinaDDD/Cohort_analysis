
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
        maxIf(__week, __event_name = '{{ var('event_return')}}') as last_week,
        --max(__week) as last_week,
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

, delta_active_users as (
    
    SELECT
        date_matrix.week_start as week_start,
        date_matrix.delta as delta,
        countIf(date_matrix.week_start + INTERVAL delta WEEK <= users_with_dates.last_week) AS active_users
    FROM date_matrix
    LEFT JOIN users_with_dates ON date_matrix.week_start = users_with_dates.first_week
    GROUP BY date_matrix.week_start, date_matrix.delta
    ORDER BY date_matrix.week_start, date_matrix.delta
)
, cohort_active_users as (
    SELECT week_start, delta, maxIf(active_users, delta = 0) OVER( PARTITION BY (week_start)) as users, active_users
    FROM delta_active_users
)

SELECT week_start, delta, users, active_users, ROUND(active_users/users, 2)*100 as active_users_percent
FROM cohort_active_users
ORDER BY 1, 2