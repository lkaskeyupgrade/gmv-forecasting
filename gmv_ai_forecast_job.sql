-- =============================================================================
-- GMV FORECAST OUTPUT -- DAILY REFRESH
-- Populates commercial_analytics.public.gmv_forecast_output
-- Run daily to keep current month + next month forecasts current
-- =============================================================================
-- =============================================================================
-- SECTION 2: AI FORECAST -- DELETE + INSERT (full atomic replace)
-- Uses Databricks ai_forecast() for partners with >= 365 days history
-- and active within the last 60 days
-- Deletes current and future forecasts then re-inserts with fresh data
-- Manual overrides are no longer applied in this section -- is_override
-- is always written as FALSE here (kept in the schema for compatibility
-- with the dashboard query and Section 3, which still supports overrides)
-- =============================================================================
DELETE FROM commercial_analytics.public.gmv_forecast_output
WHERE forecast_date >= DATE_TRUNC('MONTH', CURRENT_DATE);

INSERT INTO commercial_analytics.public.gmv_forecast_output
    (partner_grouping_legacy, forecast_date, gmv_forecast, gmv_forecast_lower,
     gmv_forecast_upper, forecast_horizon, forecast_run_ts, is_override)

WITH

-- Identify partners eligible for ai_forecast():
-- must have >= 365 days of data and been active within last 60 days
eligible_partners AS (
    SELECT partner_grouping_legacy
    FROM commercial_analytics.public.forecasting
    WHERE
        loan_date_etz      >= DATEADD(YEAR, -2, CURRENT_DATE)
        AND loan_date_etz   <  CURRENT_DATE
        AND gmv_amount_usd IS NOT NULL
        AND gmv_amount_usd  >= 0
        AND partner_grouping_legacy IS NOT NULL
    GROUP BY partner_grouping_legacy
    HAVING
        COUNT(*)               >= 365
        AND MAX(loan_date_etz) >= DATEADD(DAY, -60, CURRENT_DATE)
),

-- Build daily GMV time series for eligible partners
-- This is the input table for ai_forecast()
daily_gmv AS (
    SELECT
        loan_date_etz           AS time,
        partner_grouping_legacy AS group_col,
        SUM(gmv_amount_usd)     AS value
    FROM commercial_analytics.public.forecasting
    WHERE
        loan_date_etz      >= DATEADD(YEAR, -2, CURRENT_DATE)
        AND loan_date_etz   <  CURRENT_DATE
        AND gmv_amount_usd IS NOT NULL
        AND gmv_amount_usd  >= 0
        AND partner_grouping_legacy IN (SELECT partner_grouping_legacy FROM eligible_partners)
    GROUP BY loan_date_etz, partner_grouping_legacy
),

-- Run Databricks ai_forecast() to generate daily forecasts
-- through end of next month for all eligible partners
forecast_raw AS (
    SELECT *
    FROM ai_forecast(
        TABLE(SELECT * FROM daily_gmv),
        horizon   => LAST_DAY(DATEADD(MONTH, 1, CURRENT_DATE)),
        frequency => 'day',
        time_col  => 'time',
        value_col => 'value',
        group_col => 'group_col'
    )
),

-- Label each forecast date as current_month or next_month
-- and clip negative forecasts to zero
forecast_labeled AS (
    SELECT
        group_col                                AS partner_grouping_legacy,
        CAST(time AS DATE)                       AS forecast_date,
        GREATEST(ROUND(value_forecast, 2), 0)    AS gmv_forecast,
        GREATEST(ROUND(value_lower,    2), 0)    AS gmv_forecast_lower,
        GREATEST(ROUND(value_upper,    2), 0)    AS gmv_forecast_upper,
        CASE
            WHEN CAST(time AS DATE) < DATE_TRUNC('MONTH', DATEADD(MONTH, 1, CURRENT_DATE))
            THEN 'current_month'
            ELSE 'next_month'
        END                                      AS forecast_horizon
    FROM forecast_raw
    WHERE CAST(time AS DATE) >= DATE_TRUNC('MONTH', CURRENT_DATE)
),

-- Final output: passthrough of the raw model forecast
-- Manual overrides are not applied in this section (removed) --
-- is_override is hardcoded to FALSE since no override can be in effect here
forecast_final AS (
    SELECT
        fl.partner_grouping_legacy,
        fl.forecast_date,
        fl.gmv_forecast,
        fl.gmv_forecast_lower,
        fl.gmv_forecast_upper,
        fl.forecast_horizon,
        CURRENT_TIMESTAMP()                      AS forecast_run_ts,
        FALSE                                     AS is_override
    FROM forecast_labeled fl
)

SELECT
    partner_grouping_legacy,
    forecast_date,
    gmv_forecast,
    gmv_forecast_lower,
    gmv_forecast_upper,
    forecast_horizon,
    forecast_run_ts,
    is_override
FROM forecast_final;


-- =============================================================================
-- SECTION 3: FALLBACK FORECAST -- INSERT (appends partners not in section 2)
-- For partners with < 365 days history or inactive > 60 days
-- Uses weighted moving average with day-of-week adjustment
-- Also supports manual overrides
-- =============================================================================
INSERT INTO commercial_analytics.public.gmv_forecast_output
    (partner_grouping_legacy, forecast_date, gmv_forecast, gmv_forecast_lower,
     gmv_forecast_upper, forecast_horizon, forecast_run_ts, is_override)

WITH

-- Pull manual overrides
partner_overrides AS (
    SELECT
        partner_grouping_legacy  AS partner_name,
        monthly_gmv_override,
        apply_to
    FROM commercial_analytics.public.gmv_forecast_overrides
),

-- Partners already written by section 2 -- exclude from fallback
already_forecast AS (
    SELECT DISTINCT partner_grouping_legacy
    FROM commercial_analytics.public.gmv_forecast_output
    WHERE forecast_run_ts >= DATE_TRUNC('DAY', CURRENT_TIMESTAMP())
),

-- Partners that don't qualify for ai_forecast()
-- either too little history or inactive for > 60 days
excluded_partners AS (
    SELECT partner_grouping_legacy
    FROM commercial_analytics.public.forecasting
    WHERE
        loan_date_etz      >= DATEADD(YEAR, -3, CURRENT_DATE)
        AND loan_date_etz   <  CURRENT_DATE
        AND gmv_amount_usd IS NOT NULL
        AND gmv_amount_usd  >= 0
        AND partner_grouping_legacy IS NOT NULL
        AND partner_grouping_legacy NOT IN (SELECT partner_grouping_legacy FROM already_forecast)
    GROUP BY partner_grouping_legacy
    HAVING
        COUNT(*)              < 365
        OR MAX(loan_date_etz) < DATEADD(DAY, -60, CURRENT_DATE)
),

-- Pull last 90 days of actuals for fallback partners
-- with recency rank for weighted average calculation
recent_actuals AS (
    SELECT
        f.partner_grouping_legacy,
        f.loan_date_etz,
        f.gmv_amount_usd,
        f.day_of_week,
        ROW_NUMBER() OVER (
            PARTITION BY f.partner_grouping_legacy
            ORDER BY f.loan_date_etz DESC
        )                    AS recency_rank,
        COUNT(*) OVER (
            PARTITION BY f.partner_grouping_legacy
        )                    AS total_days
    FROM commercial_analytics.public.forecasting f
    WHERE
        f.partner_grouping_legacy IN (SELECT partner_grouping_legacy FROM excluded_partners)
        AND f.loan_date_etz >= DATEADD(DAY, -90, CURRENT_DATE)
        AND f.loan_date_etz  <  CURRENT_DATE
        AND f.gmv_amount_usd IS NOT NULL
        AND f.gmv_amount_usd >= 0
),

-- Calculate weighted moving average -- more recent days get higher weight
weighted_avg AS (
    SELECT
        partner_grouping_legacy,
        SUM(gmv_amount_usd * (total_days - recency_rank + 1))
            / NULLIF(SUM(total_days - recency_rank + 1), 0)  AS wma_daily_gmv
    FROM recent_actuals
    GROUP BY partner_grouping_legacy
),

-- Calculate day-of-week multipliers to adjust for weekly seasonality
-- e.g. Mondays might be 1.2x average, Sundays 0.7x
dow_index AS (
    SELECT
        partner_grouping_legacy,
        day_of_week,
        AVG(gmv_amount_usd) /
            NULLIF(
                AVG(AVG(gmv_amount_usd)) OVER (PARTITION BY partner_grouping_legacy)
            , 0)             AS dow_multiplier
    FROM commercial_analytics.public.forecasting
    WHERE
        partner_grouping_legacy IN (SELECT partner_grouping_legacy FROM excluded_partners)
        AND loan_date_etz >= DATEADD(DAY, -90, CURRENT_DATE)
        AND loan_date_etz  <  CURRENT_DATE
        AND gmv_amount_usd IS NOT NULL
        AND gmv_amount_usd >= 0
    GROUP BY partner_grouping_legacy, day_of_week
),

-- Generate one row per forecast date for current and next month
forecast_dates AS (
    SELECT EXPLODE(
        SEQUENCE(
            DATE_TRUNC('MONTH', CURRENT_DATE),
            LAST_DAY(DATEADD(MONTH, 1, CURRENT_DATE)),
            INTERVAL 1 DAY
        )
    ) AS forecast_date
),

-- Apply WMA * day-of-week multiplier to get daily forecast
-- confidence bands are +/- 15% of point forecast
fallback_base AS (
    SELECT
        w.partner_grouping_legacy,
        d.forecast_date,
        GREATEST(ROUND(w.wma_daily_gmv * COALESCE(di.dow_multiplier, 1.0), 2), 0)
                             AS gmv_forecast,
        GREATEST(ROUND(w.wma_daily_gmv * COALESCE(di.dow_multiplier, 1.0) * 0.85, 2), 0)
                             AS gmv_forecast_lower,
        GREATEST(ROUND(w.wma_daily_gmv * COALESCE(di.dow_multiplier, 1.0) * 1.15, 2), 0)
                             AS gmv_forecast_upper,
        CASE
            WHEN d.forecast_date < DATE_TRUNC('MONTH', DATEADD(MONTH, 1, CURRENT_DATE))
            THEN 'current_month'
            ELSE 'next_month'
        END                  AS forecast_horizon
    FROM weighted_avg w
    CROSS JOIN forecast_dates d
    LEFT JOIN dow_index di
        ON  w.partner_grouping_legacy  = di.partner_grouping_legacy
        AND DAYOFWEEK(d.forecast_date) = di.day_of_week
),

-- Calculate day weights for override distribution
fallback_with_weights AS (
    SELECT
        fb.*,
        gmv_forecast / NULLIF(
            SUM(gmv_forecast) OVER (PARTITION BY partner_grouping_legacy, forecast_horizon)
        , 0)                 AS day_weight,
        SUM(gmv_forecast) OVER (PARTITION BY partner_grouping_legacy, forecast_horizon)
                             AS model_monthly_total
    FROM fallback_base fb
),

-- Apply manual overrides where present
fallback_final AS (
    SELECT
        fw.partner_grouping_legacy,
        fw.forecast_date,

        CASE
            WHEN po.monthly_gmv_override IS NOT NULL
             AND (po.apply_to = 'both' OR po.apply_to = fw.forecast_horizon)
            THEN GREATEST(ROUND(po.monthly_gmv_override * fw.day_weight, 2), 0)
            ELSE fw.gmv_forecast
        END                  AS gmv_forecast,

        CASE
            WHEN po.monthly_gmv_override IS NOT NULL
             AND (po.apply_to = 'both' OR po.apply_to = fw.forecast_horizon)
            THEN GREATEST(ROUND(
                     fw.gmv_forecast_lower
                     * (po.monthly_gmv_override / NULLIF(fw.model_monthly_total, 0))
                 , 2), 0)
            ELSE fw.gmv_forecast_lower
        END                  AS gmv_forecast_lower,

        CASE
            WHEN po.monthly_gmv_override IS NOT NULL
             AND (po.apply_to = 'both' OR po.apply_to = fw.forecast_horizon)
            THEN GREATEST(ROUND(
                     fw.gmv_forecast_upper
                     * (po.monthly_gmv_override / NULLIF(fw.model_monthly_total, 0))
                 , 2), 0)
            ELSE fw.gmv_forecast_upper
        END                  AS gmv_forecast_upper,

        fw.forecast_horizon,
        CURRENT_TIMESTAMP()  AS forecast_run_ts,

        CASE
            WHEN po.monthly_gmv_override IS NOT NULL
             AND (po.apply_to = 'both' OR po.apply_to = fw.forecast_horizon)
            THEN TRUE
            ELSE FALSE
        END                  AS is_override

    FROM fallback_with_weights fw
    LEFT JOIN partner_overrides po
        ON fw.partner_grouping_legacy = po.partner_name
)

SELECT
    partner_grouping_legacy,
    forecast_date,
    gmv_forecast,
    gmv_forecast_lower,
    gmv_forecast_upper,
    forecast_horizon,
    forecast_run_ts,
    is_override
FROM fallback_final;


-- =============================================================================
-- SECTION 4: MONTH-START SNAPSHOT
-- Creates a point-in-time snapshot of the forecast at the start of each month
-- Table name format: forecast_MMDDYYYY (e.g. forecast_06012026)
-- Used to compare month-start forecast vs actual month-end results
-- Safe to run daily -- overwrites the same snapshot until month rolls over
-- =============================================================================
EXECUTE IMMEDIATE
    'CREATE OR REPLACE TABLE commercial_analytics.commercial_analytics.forecast_'
    || DATE_FORMAT(DATE_TRUNC('MONTH', CURRENT_DATE), 'MMddyyyy')
    || ' AS
    SELECT
        partner_grouping_legacy,
        forecast_date,
        gmv_forecast             AS gmv_forecast_month_start,
        gmv_forecast_lower       AS gmv_forecast_lower_month_start,
        gmv_forecast_upper       AS gmv_forecast_upper_month_start,
        forecast_horizon,
        forecast_run_ts          AS snapshot_ts
    FROM commercial_analytics.public.gmv_forecast_output
    WHERE forecast_horizon = \'current_month\'';
