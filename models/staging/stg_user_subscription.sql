{{ config(materialized = 'table') }}

WITH src_user_subscriptions AS (
        SELECT  load_id,
                exported_at,
                LEAD(load_id)  OVER (PARTITION BY user_id ORDER BY exported_at, load_id)      AS next_load_id,
                LEAD(exported_at)  OVER (PARTITION BY user_id ORDER BY exported_at, load_id)  AS next_exported_at,
                user_id,
                subscriptions,
                ARRAY_SIZE(subscriptions)                                                     AS subscriptions_size
        FROM (
                    SELECT id                                                    AS load_id,
                           record:id::NUMBER                                     AS user_id,
                           TO_TIMESTAMP(record:exported_at::NUMBER,3)            AS exported_at,
                           record:subscriptions::VARIANT                         AS subscriptions
                    FROM {{ source('dbt_video_platform', 'raw_user_subscription') }}
        )
), user_subscription AS (
        SELECT src.load_id,
               src.exported_at,
               src.next_load_id,
               src.next_exported_at,
               src.user_id,
               subscription.value:id::NUMBER                              AS subscription_id,
               subscription.index                                         AS subscription_index,
               subscription.value:name::VARCHAR                           AS subscription_name,
               TO_TIMESTAMP(subscription.value:subscribed_at::NUMBER,3)   AS subscription_subscribed_at,
               src.subscriptions_size
        FROM src_user_subscriptions src
        LEFT OUTER JOIN TABLE(FLATTEN(input => src.subscriptions)) subscription ON 1=1
        WHERE subscriptions_size > 0
)

SELECT load_id,
       exported_at,
       user_id,
       subscription_id,
       subscription_index,
       subscription_name,
       subscription_subscribed_at,
       subscriptions_size,
       FALSE                                     AS _deleted
FROM user_subscription
WHERE subscriptions_size> 0

UNION ALL

-- create artificially deleted entries for the subscriptions which don't exist in
-- the next staging occurrence of the user's subscriptions
SELECT next_load_id                             AS load_id,
       next_exported_at                         AS exported_at,
       user_id,
       subscription_id,
       NULL                                     AS subscription_index,
       NULL                                     AS subscription_name,
       NULL                                     AS subscription_subscribed_at,
       NULL                                     AS subscriptions_size,
       TRUE                                     AS _deleted
FROM user_subscription   AS current_user_subscription
WHERE next_load_id IS NOT NULL
  AND NOT EXISTS (
        SELECT 1 FROM user_subscription AS next_user_subscription
        WHERE current_user_subscription.next_load_id = next_user_subscription.load_id
          AND current_user_subscription.subscription_id = next_user_subscription.subscription_id
  )