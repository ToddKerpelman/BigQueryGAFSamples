SELECT countif(funnel_start_time is not null) as funnel_begin_count, 
       countif(funnel_end_time - funnel_start_time < 4 * 60 * 60 * 1000 * 1000)
         as funnel_end_count 
FROM (
  SELECT funnel_start_time,
  LEAD(funnel_end_time, 1) 
    OVER (PARTITION BY app_instance_id ORDER BY event_time) AS funnel_end_time 
  FROM (
    SELECT event.name,
    if (event.name = "tutorial_begin", event.timestamp_micros, null) as funnel_start_time,
    if (event.name = "tutorial_complete", event.timestamp_micros, null) as funnel_end_time,
    user_dim.app_info.app_instance_id,
    event.timestamp_micros as event_time
    FROM `my_datset.app_events_*`,
    UNNEST(event_dim) as event
    WHERE event.name = "tutorial_begin" OR
    event.name = "tutorial_complete"
    AND _TABLE_SUFFIX BETWEEN '20171001' AND '20171007'
    ORDER BY app_instance_id, event.timestamp_micros
  )
)
