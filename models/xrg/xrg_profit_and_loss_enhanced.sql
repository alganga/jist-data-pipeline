WITH MAX_DATE AS (
    SELECT
        MAX(date_trunc('month', (year || '-' || month || '-01')::date)) AS max_date
    FROM {{ref('xrg_profit_and_loss')}}
),

-- Aggregate data from the profit and loss table.
PROFIT_AND_LOSS AS (
    SELECT
        PL.concept_id,
        PL.concept_name,
        PL.concept,
        PL.entity_id,
        PL.entity_name,
        PL.source_entity_id,
        PL.metric_name,
        EPC.entity_period_id,
        PL.period,
        PL.year::int AS year,
        PL.month::int AS month,
        SUM(PL.ending_balance) AS pnl_ending_balance,
        SUM(PL.actual_mtd) AS pnl_actual,
        SUM(PL.actual_ytd) AS pnl_actual_ytd,
        SUM(PL.actual_ttm) AS pnl_actual_ttm,
        SUM(PL.budget) AS pnl_budget,
        SUM(PL.budget_mtd) AS pnl_budget_mtd,
        SUM(PL.budget_ytd) AS pnl_budget_ytd,
        SUM(PL.budget_ttm) AS pnl_budget_ttm
    FROM {{ref('xrg_profit_and_loss')}} PL
    LEFT JOIN {{source('xrg', 'entity_period_config')}} EPC
        ON PL.entity_id = EPC.entity_id
        AND PL.period = EPC.period
    JOIN MAX_DATE MD ON date_trunc('month', (PL.year || '-' || PL.month || '-01')::date) <= MD.max_date
    WHERE date_trunc('month', (PL.year || '-' || PL.month || '-01')::date) >= (MD.max_date - interval '11 months')
    GROUP BY PL.concept_id, PL.concept_name, PL.concept, PL.entity_id, PL.entity_name,
    PL.source_entity_id, PL.metric_name, EPC.entity_period_id, PL.period, PL.year, PL.month
)

SELECT * FROM PROFIT_AND_LOSS 
WHERE metric_name in ('Revenue', 'EBITDA', 'Gross Profit')
