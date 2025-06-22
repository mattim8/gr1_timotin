-- 1. Создание таблицы с сырыми логами, используя движок MergeTree с ORDER BY по первичномуключу (event_time, user_id)
--    что поможет быстрее находить данные. Сначала сортировка идет по дате(удобно), а потом по user id, в конкретном 
--    случае- частое использование данных пользователя в определенные даты.

CREATE TABLE user_events(
	user_id UInt32,
	event_type String,
	points_spent UInt32,
	event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;

-- 2. Создание агрегированной таблицы, используется движок AggregatingMergeTree для поддержки сложных
--    агр. функций, а для нужных полей добавляем AggregateFunction для более эффективного хранения
--    промежуточных состояний агрегации(не готовых результатов), а в случае вставки новых данных может
--    объединить без пересчета с нуля(при merge). TTL(время жизни) - возможность какого-либо
--    (в нашем случае хранения) данных до определенного срока. 

CREATE TABLE aggr_user_events(
	event_date Date,
	event_type String,
	unique_users AggregateFunction(uniq, UInt32),
	total_points AggregateFunction(sum, UInt32),
	event_count AggregateFunction(count, UInt32)
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;

-- 3. Создание Materialized View. Здесь в качестве триггера для агр. таблицы, подготавливает и обновляет
--    данные. Без движка, так как есть связь с таблицей(TO ...) с необходимым движком. State функции 
--    вычисляют промежуточное агр. состояни для конкретных операций(uniq, sum, count). Группировка
--    позволяет анализировать данные по дням и видам активности.

CREATE MATERIALIZED VIEW mv TO aggr_user_events
AS
SELECT toDate(event_time) as event_date,
	   event_type,
	   uniqState(user_id) as unique_users,
	   sumState(points_spent) as total_points,
	   countState() as event_count
FROM user_events
GROUP BY event_date, 
         event_type;

-- 4. Запрос с теми же группировками. Используем merge, чтобы объединить агрегатные состояния с ф-циями
--    State и получить итоговый результат, без этого же данные будут в необработанном виде(бинарном).

SELECT   event_date, 
         event_type,
         uniqMerge(unique_users) as unique_users,
         sumMerge(total_points) as total_points,
         countMerge(event_count) as total_actions
FROM aggr_user_events
GROUP BY event_date, 
         event_type
ORDER BY event_date;

-- 5. Из таблицы сырых логов берем минимальную дату для каждого id- это и будет день 0(первый день),
--    подзапрос first_event. Добавляем через left join остальные события и называем later_event, чтобы
--    проверить действия пользователя после первого дня. В SELECT преобразуем время ивента в простую дату,
--    считаем уникальных пользователей первого дня(через функцию CH). Потом countDistinctIf(функция CH)
--    фильтрует по диапазону от 1 до 7 дней. Ниже определяем retention(сколько процентов польз-лей
--    вернулись в течение 7 дней). По формуле retention_7d_percent - сколько уникальных вернулись
--    в теч. 7д делим на кол-во пришедших в день 0.
--    toDate(later_event.event_time) > toDate(first_event.event_time) --> исключаем день 0, так как нужно
--    возвращение. 
--    toDate(later_event.event_time) <= toDate(first_event.event_time) + 7 --> вкл 7 дней после первого события.

SELECT 
    toDate(first_event.event_time) AS day_0,
    countDistinct(first_event.user_id) AS total_users_day_0,
    countDistinctIf(later_event.user_id,
        toDate(later_event.event_time) > toDate(first_event.event_time) AND
        toDate(later_event.event_time) <= toDate(first_event.event_time) + 7
    ) AS returned_in_7_days,
    round(
        100.0 * countDistinctIf(later_event.user_id,
            toDate(later_event.event_time) > toDate(first_event.event_time) AND
            toDate(later_event.event_time) <= toDate(first_event.event_time) + 7
        ) / countDistinct(first_event.user_id),
        2
    ) AS retention_7d_percent
FROM 
    (
        SELECT 
            user_id,
            min(event_time) AS event_time
        FROM user_events
        GROUP BY user_id
    ) AS first_event
LEFT JOIN user_events AS later_event
    ON first_event.user_id = later_event.user_id
GROUP BY toDate(first_event.event_time)
ORDER BY day_0;
