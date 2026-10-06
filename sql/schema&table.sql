create schema stg;
create schema cdm;


create table stg.orders_dataset(
	order_id text,
	customer_id text,
	order_status text,
	order_purchase_timestamp timestamp,
	order_approved_at timestamp,
	order_delivered_carrier_date timestamp,
	order_delivered_customer_date timestamp,
	order_estimated_delivery_date date
)
distributed randomly;

select * from stg.orders_dataset;

select count(*) from stg.orders_dataset;