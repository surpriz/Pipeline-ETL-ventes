-- Dans src/sql/analysis_views.sql

-- 1. Vue pour le chiffre d'affaires quotidien
CREATE OR REPLACE VIEW daily_revenue AS
SELECT 
    dt.date_actual,
    COUNT(DISTINCT fo.order_id) as number_of_orders,
    SUM(fo.total_amount) as total_revenue
FROM fact_orders fo
JOIN dim_time dt ON fo.date_id = dt.date_id
GROUP BY dt.date_actual
ORDER BY dt.date_actual;

-- 2. Vue pour le chiffre d'affaires mensuel
CREATE OR REPLACE VIEW monthly_revenue AS
SELECT 
    dt.year,
    dt.month,
    COUNT(DISTINCT fo.order_id) as number_of_orders,
    SUM(fo.total_amount) as total_revenue,
    AVG(fo.total_amount) as average_order_value
FROM fact_orders fo
JOIN dim_time dt ON fo.date_id = dt.date_id
GROUP BY dt.year, dt.month
ORDER BY dt.year, dt.month;

-- 3. Vue pour les commandes par région
CREATE OR REPLACE VIEW orders_by_region AS
SELECT 
    dg.state,
    dg.city,
    COUNT(DISTINCT fo.order_id) as number_of_orders,
    COUNT(DISTINCT dc.customer_id) as number_of_customers,
    SUM(fo.total_amount) as total_revenue
FROM fact_orders fo
JOIN dim_customers dc ON fo.customer_id = dc.customer_id
JOIN dim_geography dg ON dc.geography_id = dg.geography_id
GROUP BY dg.state, dg.city
ORDER BY number_of_orders DESC;

-- 4. Vue pour la performance des vendeurs
CREATE OR REPLACE VIEW seller_performance AS
SELECT 
    ds.seller_id,
    dg.state as seller_state,
    dg.city as seller_city,
    COUNT(DISTINCT foi.order_id) as total_orders,
    COUNT(DISTINCT dp.product_id) as unique_products_sold,
    SUM(foi.price) as total_revenue,
    AVG(foi.price) as average_order_value,
    SUM(foi.freight_value) as total_freight_value
FROM fact_order_items foi
JOIN dim_sellers ds ON foi.seller_id = ds.seller_id
JOIN dim_geography dg ON ds.geography_id = dg.geography_id
JOIN dim_products dp ON foi.product_id = dp.product_id
GROUP BY ds.seller_id, dg.state, dg.city
ORDER BY total_revenue DESC;

-- 5. Vue pour les délais de livraison
CREATE OR REPLACE VIEW delivery_performance AS
SELECT 
    dt.year,
    dt.month,
    dg.state,
    AVG(fo.delivery_delay) as avg_delivery_days,
    MIN(fo.delivery_delay) as min_delivery_days,
    MAX(fo.delivery_delay) as max_delivery_days,
    COUNT(CASE WHEN fo.delivery_delay > 7 THEN 1 END) as deliveries_over_week
FROM fact_orders fo
JOIN dim_time dt ON fo.date_id = dt.date_id
JOIN dim_customers dc ON fo.customer_id = dc.customer_id
JOIN dim_geography dg ON dc.geography_id = dg.geography_id
WHERE fo.delivery_delay IS NOT NULL
GROUP BY dt.year, dt.month, dg.state
ORDER BY dt.year, dt.month, avg_delivery_days DESC;