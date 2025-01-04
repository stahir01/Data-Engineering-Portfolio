WITH daily_expense_summary AS (
    SELECT
        transaction_date AS report_date,
        SUM(fixed_budget) AS fixed_costs,
        SUM(variable_budget) AS dynamic_costs
    FROM expense_tracker
    GROUP BY report_date
),
supply_chain_overheads AS (
    SELECT
        order_date AS report_date,
        SUM(net_sales) * 0.065 AS supply_chain_costs
    FROM transaction_log
    GROUP BY report_date
),
customer_retention_expense AS (
    SELECT
        order_date AS report_date,
        SUM(net_sales) * 0.25 AS retention_cost
    FROM transaction_log
    WHERE customer_type = 'Loyal'
    GROUP BY report_date
),
acquisition_cost_summary AS (
    SELECT
        report_date,
        acquisition_cost
    FROM (
        SELECT
            order_date AS report_date,
            SUM(net_sales) * 0.33 + 8.5 AS acquisition_cost,
            ROW_NUMBER() OVER (PARTITION BY order_date ORDER BY order_date) AS rank
        FROM transaction_log
        WHERE customer_type = 'New'
        GROUP BY order_date
    ) subquery
    WHERE rank = 1
),
advertisement_impact AS (
    SELECT
        campaign_date AS report_date,
        SUM(clicks) AS total_clicks,
        SUM(views) AS total_views,
        CASE
            WHEN SUM(views) > 0 THEN (SUM(clicks) * 1.0 / SUM(views))
            ELSE 0
        END AS engagement_rate
    FROM ad_campaigns
    GROUP BY report_date
),
product_performance AS (
    SELECT
        sale_date AS report_date,
        product_category,
        SUM(sale_value) AS total_sales,
        SUM(units_sold) AS total_units
    FROM sales_data
    GROUP BY report_date, product_category
),
final_cost_and_revenue_analysis AS (
    SELECT
        des.report_date,
        des.fixed_costs + des.dynamic_costs AS total_expenses,
        sco.supply_chain_costs,
        cre.retention_cost,
        acs.acquisition_cost,
        ai.total_clicks,
        ai.engagement_rate,
        pp.total_sales,
        pp.total_units,
        (des.fixed_costs + sco.supply_chain_costs + cre.retention_cost + acs.acquisition_cost) AS total_operational_cost,
        (pp.total_sales - (des.fixed_costs + sco.supply_chain_costs + cre.retention_cost + acs.acquisition_cost)) AS net_profit
    FROM daily_expense_summary des
    LEFT JOIN supply_chain_overheads sco ON des.report_date = sco.report_date
    LEFT JOIN customer_retention_expense cre ON des.report_date = cre.report_date
    LEFT JOIN acquisition_cost_summary acs ON des.report_date = acs.report_date
    LEFT JOIN advertisement_impact ai ON des.report_date = ai.report_date
    LEFT JOIN product_performance pp ON des.report_date = pp.report_date
)
SELECT
    report_date,
    total_expenses,
    supply_chain_costs,
    retention_cost,
    acquisition_cost,
    total_clicks,
    engagement_rate,
    total_sales,
    total_units,
    total_operational_cost,
    net_profit
FROM final_cost_and_revenue_analysis
ORDER BY report_date;
