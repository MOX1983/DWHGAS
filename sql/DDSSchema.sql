create schema dds;

create table dds.fact_orders(
	order_id text,
	order_item_id int,
	product_id text,
	price decimal(10, 2),
	freight_value decimal(10, 2),
	customer_id text,
	seller_id text,
	logical_date timestamp
)
distributed by (order_id);


create table dds.fact_payments(
	order_id text,
	payment_sequential int,
	payment_type text,
	payment_installments int,
	payment_value decimal(10, 2),
	logical_date timestamp
)
distributed by (order_id);


create table dds.fact_reviews(
	review_id text,
	order_id text,
	review_score int,
	review_comment_title text,
	review_comment_message text,
	review_creation_date timestamp,
	review_answer_timestamp timestamp,
	logical_date timestamp
)
distributed by (order_id);

create table dds.dim_orders(
	order_id text,
	order_status text,
	order_approved_at timestamp,
	order_delivered_carrier_date timestamp,
	order_delivered_customer_date timestamp,
	order_estimated_delivery_date date, 
	order_purchase_timestamp timestamp,
	shipping_limit_date timestamp,
	logical_date timestamp
)
distributed by (order_id);

create table dds.dim_products(
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
distributed by (product_id);

create table dds.dim_customers(
	customer_id text,
	customer_unique_id text,
	customer_zip_code_prefix int,
	customer_city text,
	customer_state text,
	logical_date timestamp
)
distributed by (customer_id);

create table dds.dim_sellers(
	seller_id text,
	seller_zip_code_prefix int,
	seller_city text,
	seller_state text,
	logical_date timestamp
)
distributed by (seller_id);


create table dds.dim_geolocation(
	geolocation_zip_code_prefix int,
	geolocation_lat double precision,
	geolocation_lng double precision,
	geolocation_city text,
	geolocation_state text, 
	logical_date timestamp
)
distributed by (geolocation_city);


select count(*)
from dds.fact_orders fo ;

select count(*)
from dds.dim_orders dio ;

select sum(fo.price + fo.freight_value )
from dds.fact_orders fo 
where fo.logical_date = '2026-10-16 13:59:07.000';


select order_id, count(*) as cnt
from dds.dim_orders
where logical_date = '2026-10-16 13:59:07.000'
group by order_id
having count(*) > 1
order by cnt desc
limit 20;


select max(fo.logical_date  )
from dds.fact_orders fo ;

delete from dds.dim_customers;
delete from dds.dim_geolocation;
delete from dds.dim_orders ;
delete from dds.dim_products ;
delete from dds.dim_sellers ;
delete from dds.fact_orders ;
delete from dds.fact_payments ;
delete from dds.fact_reviews ;




