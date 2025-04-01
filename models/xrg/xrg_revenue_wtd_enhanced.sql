WITH revenue_wtd_base AS (
    SELECT * from {{ref('xrg_revenue_wtd')}}
)

SELECT
    epc.entity_period_id,
    rev.*
FROM revenue_wtd_base rev
LEFT JOIN {{source('xrg', 'entity_period_config')}} epc
    ON rev.entity_id = epc.entity_id
    AND rev.year = epc.year
    AND rev.month = epc.month
