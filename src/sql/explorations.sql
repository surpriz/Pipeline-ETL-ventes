-- Requête 1 : Chiffre d'affaires par produit (TOP 10 des produits les plus vendus)
-- 👉 Objectif : Identifier les produits les plus performants en termes de ventes.

SELECT 
    dp.product_id,
    dp.category_name,
    COUNT(DISTINCT foi.order_id) AS total_orders,
    SUM(foi.price) AS total_revenue,
    AVG(foi.price) AS avg_order_value
FROM fact_order_items foi
JOIN dim_products dp ON foi.product_id = dp.product_id
GROUP BY dp.product_id, dp.category_name
ORDER BY total_revenue DESC
LIMIT 10;


-- 🔹 Requête 2 : Analyse des vendeurs (TOP vendeurs par chiffre d'affaires)
-- 👉 Objectif : Évaluer les vendeurs les plus rentables.

SELECT 
    sp.seller_id,
    sp.seller_state,
    sp.total_orders,
    sp.total_revenue,
    sp.average_order_value
FROM seller_performance sp
ORDER BY sp.total_revenue DESC
LIMIT 10;


--🔹 Requête 3 : Nombre de commandes par région (focus état/ville)
-- 👉 Objectif : Voir quelles régions sont les plus dynamiques en termes de volume de commandes.

SELECT 
    obr.state,
    obr.city,
    obr.number_of_orders,
    obr.total_revenue
FROM orders_by_region obr
ORDER BY obr.number_of_orders DESC;

-- 🔹 Requête 4 : Chiffre d'affaires trimestriel
-- 👉 Objectif : Suivre les performances commerciales par trimestre.

SELECT 
    dt.year,
    dt.quarter,
    SUM(fo.total_amount) AS total_revenue,
    COUNT(DISTINCT fo.order_id) AS number_of_orders
FROM fact_orders fo
JOIN dim_time dt ON fo.date_id = dt.date_id
GROUP BY dt.year, dt.quarter
ORDER BY dt.year, dt.quarter;

-- Requête 5 : Croissance annuelle du chiffre d'affaires
-- Objectif : Calculer la croissance annuelle et comparer avec l'année précédente.

SELECT 
    dt.year,
    SUM(fo.total_amount) AS total_revenue,
    LAG(SUM(fo.total_amount)) OVER (ORDER BY dt.year) AS previous_year_revenue,
    ROUND(((SUM(fo.total_amount) - LAG(SUM(fo.total_amount)) OVER (ORDER BY dt.year)) / 
    NULLIF(LAG(SUM(fo.total_amount)) OVER (ORDER BY dt.year), 0)) * 100, 2) AS growth_rate
FROM fact_orders fo
JOIN dim_time dt ON fo.date_id = dt.date_id
GROUP BY dt.year
ORDER BY dt.year;


-- Requête 6 : Délai moyen par état (TOP 5 plus longs délais)
-- Objectif : Identifier les zones où les délais de livraison sont problématiques.

SELECT 
    dp.state,
    AVG(fo.delivery_delay) AS avg_delivery_days,
    COUNT(fo.order_id) AS total_orders
FROM fact_orders fo
JOIN dim_customers dc ON fo.customer_id = dc.customer_id
JOIN dim_geography dp ON dc.geography_id = dp.geography_id
WHERE fo.delivery_delay IS NOT NULL
GROUP BY dp.state
ORDER BY avg_delivery_days DESC
LIMIT 5;


-- Requête 7 : Délais de livraison moyens par vendeur
-- Objectif : Comparer les délais de livraison par vendeur pour détecter des inefficacités.

SELECT 
    ds.seller_id,
    dg.state AS seller_state,
    AVG(fo.delivery_delay) AS avg_delivery_days,
    COUNT(fo.order_id) AS total_orders
FROM fact_orders fo
JOIN fact_order_items foi ON fo.order_id = foi.order_id
JOIN dim_sellers ds ON foi.seller_id = ds.seller_id
JOIN dim_geography dg ON ds.geography_id = dg.geography_id
WHERE fo.delivery_delay IS NOT NULL
GROUP BY ds.seller_id, dg.state
ORDER BY avg_delivery_days DESC;
