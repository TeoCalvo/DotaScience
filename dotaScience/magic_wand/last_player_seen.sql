WITH TB_FLAG_PLAYER AS (

    SELECT *
           ,ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY dt_ref DESC) as dt_update
    FROM {table} -- NOSSA HISTÓRIA DE CADA PLAYER

)

SELECT *
FROM TB_FLAG_PLAYER
WHERE dt_update = 1