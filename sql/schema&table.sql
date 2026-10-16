create schema stg;



create table stg.orders_dataset(
	order_id text,
	customer_id text,
	order_status text,
	order_purchase_timestamp timestamp,
	order_approved_at timestamp,
	order_delivered_carrier_date timestamp,
	order_delivered_customer_date timestamp,
	order_estimated_delivery_date date,
	logical_date timestamp
)
distributed randomly;

create table stg.customers_dataset(
	customer_id text,
	customer_unique_id text,
	customer_zip_code_prefix int,
	customer_city text,
	customer_state text,
	logical_date timestamp
)
distributed randomly;

create table stg.order_items_dataset(
	order_id text,
	order_item_id int,
	product_id text,
	seller_id text,
	shipping_limit_date timestamp,
	price decimal(10, 2),
	freight_value decimal(10, 2),
	logical_date timestamp
)
distributed randomly;

create table stg.products_dataset(
	product_id text,
	product_category_name text,
	product_name_lenght int,
	product_description_lenght int,
	product_photos_qty int,
	product_weight_g int, 
	product_length_cm int, 
	product_height_cm int, 
	product_width_cm int,
	logical_date timestamp
)
distributed randomly;

create table stg.sellers_dataset(
	seller_id text,
	seller_zip_code_prefix int,
	seller_city text,
	seller_state text,
	logical_date timestamp
)
distributed randomly;

create table stg.geolocation_dataset(
	geolocation_zip_code_prefix int,
	geolocation_lat double precision,
	geolocation_lng double precision,
	geolocation_city text,
	geolocation_state text,
	logical_date timestamp
)
distributed randomly;

create table stg.order_payments_dataset(
	order_id text,
	payment_sequential int,
	payment_type text,
	payment_installments int,
	payment_value decimal(10, 2),
	logical_date timestamp
)
distributed randomly;

create table stg.order_reviews_dataset(
	review_id text,
	order_id text,
	review_score int,
	review_comment_title text,
	review_comment_message text,
	review_creation_date timestamp,
	review_answer_timestamp timestamp,
	logical_date timestamp
)
distributed randomly;

create table stg.product_category_name_translation(
	product_category_name text,
	product_category_name_english text,
	logical_date timestamp
)
distributed randomly;



select * from stg.orders_dataset;
select * from stg.customers_dataset;
select * from stg.products_dataset ;

select max(logical_date) from stg.products_dataset;
select count(*) from stg.products_dataset where logical_date = '2026-10-15 14:43:12.000';

vacuum;

delete from stg.orders_dataset;
delete from stg.customers_dataset;
delete from stg.geolocation_dataset ;
delete from stg.order_items_dataset ;
delete from stg.order_payments_dataset ;
delete from stg.order_reviews_dataset;
delete from stg.product_category_name_translation;
delete from stg.products_dataset ;
delete from stg.sellers_dataset ;


insert into dds.dim_customers (customer_id, customer_unique_id, customer_zip_code_prefix, 
	customer_city, customer_state)
select cd.customer_id, cd.customer_unique_id, cd.customer_zip_code_prefix,
	cd.customer_city, cd.customer_state 
from stg.customers_dataset cd;

analyze stg.orders_dataset;

insert into dds.fact_orders (order_id, order_item_id, product_id, price,
	freight_value, customer_id, seller_id)
select od.order_id, t.order_item_id, t.product_id, t.price,
	t.freight_value, od.customer_id, t.seller_id 
from stg.orders_dataset od 
join stg.order_items_dataset t 
on od.order_id = t.order_id and od.logical_date == t.logical_date ;


select count(*)
from stg.orders_dataset od 
join stg.order_items_dataset t 
on od.order_id = t.order_id and od.logical_date = t.logical_date
where od.logical_date = '2026-10-15 14:43:12.000';























