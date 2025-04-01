WITH RENT_ROLL_BASE AS (
    SELECT
        rent.company_id,
        rent.company_name,
        config.concept_id,
        config.concept_name,
        config.concept,
        config.entity_id,
        config.entity_name,
        rent.source_entity_id,
        rent.source_entity_name,
        rent.street,
        rent.zip_code,
        rent.city,
        rent.state,
        rent.size,
        rent.location_type,
        rent.lease_type,
        rent.rent_start,
        rent.rent_expiration,
        rent.seat_total,
        census.median_household_income,
        census.population,
        census.median_age,
        case
        when rent.date = 'NaT' then NULL
        else rent.date
        end as date,
        rent.rent_base,
        rent.rent_nnn
    FROM {{source('xrg', 'rent_roll')}} rent
    LEFT JOIN {{source('xrg', 'census')}} census
        ON rent.zip_code = census.zip_code_tabulation_area
    LEFT JOIN {{source('xrg', 'entity_config')}} config
        ON rent."source_entity_id" = CAST(config.source_entity_id AS VARCHAR)
)

SELECT 
    rent.company_id,
    rent.company_name,
    concept_id,
    concept_name,
    concept,
    epc.entity_period_id,
    rent.entity_id,
    rent.entity_name,
    source_entity_id,
    source_entity_name,
    street,
    zip_code,
    city,
    state,
    size,
    location_type,
    lease_type,
    rent_start,
    rent_expiration,
    seat_total,
    date,
    extract(year from date::timestamp) as year,
    extract(month from date::timestamp) as month,
    median_household_income,
    population,
    median_age,
    rent_base,
    rent_nnn,
    rent_base + rent_nnn as rent_total
FROM RENT_ROLL_BASE rent
LEFT JOIN {{source('xrg', 'entity_period_config')}} epc
    ON rent.entity_id = epc.entity_id
    AND extract(year from rent.date::timestamp) = epc.year
    AND extract(month from rent.date::timestamp) = epc.month
