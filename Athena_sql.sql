--0. 
CREATE TABLE order_products_prior WITH (external_location = 's3://imba/features/order_products_prior/', format = 'parquet')
as (
SELECT a.*, b.product_id,
               b.add_to_cart_order,
               b.reordered FROM orders a
JOIN order_products b
ON a.order_id = b.order_id
WHERE a.eval_set = 'prior')

--1.user_features_1
CREATE TABLE user_features_1 
with ( exiternal_location = 's3://imba00justin052/features/user_features_1/',format='parquet')
as(                           
select user_id, 
       Max(order_number) as user_orders, 
       Sum(days_since_prior_order) as user_period, 
       Avg(days_since_prior_order) as user_mean_days_since_prior
from order_products_prior
group by user_id
);


--2.user_features_2
CREATE TABLE user_features_2 WITH (external_location = 's3://imba00justin052/features/user_features_2/', format = 'parquet')
as(
select 
  user_id,
  sum(add_to_cart_order) as total_number_of_products,
  count(distinct product_id) as total_number_of_distinct_products, 
  1 / Cast(Sum(CASE WHEN order_number > 1 THEN 1 ELSE 0 END) AS DOUBLE) as user_reorder_ratio
from order_products_prior
group by user_id
);

--3.user_features_3
CREATE TABLE user_features_3 WITH (external_location = 's3://imba00justin052/features/up_features/', format = 'parquet')
as(
select sum(order_number)as total_order_number,
               max(order_number) as max_order_number,
               min(order_number) as min_order_number,
               round(avg(add_to_cart_order),2) as avg_add_to_cart_order
from order_products_prior
group by user_id,product_id
 )

--4.prd_features
CREATE TABLE prd_features WITH (external_location = 's3://imba00justin052/features/prd_features/', format = 'parquet') 
as (

SELECT product_id,
   Count(*) AS prod_orders,
   Sum(reordered) AS prod_reorders,
   Sum(CASE WHEN product_seq_time = 1 THEN 1 ELSE 0 END) AS prod_first_orders, 
   Sum(CASE WHEN product_seq_time = 2 THEN 1 ELSE 0 END) AS prod_second_orders
FROM (
   SELECT *, Rank() OVER (
       partition BY user_id, product_id ORDER BY user_id, order_number) AS product_seq_time
   FROM order_products_prior) 
GROUP BY product_id 
);
