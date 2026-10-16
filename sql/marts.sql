create schema marts;

--плохие витрины исправить 
drop view if exists marts.orders_items_payments;
create or replace view marts.orders_items_payments as 
select  fo.order_id,
		fo.order_item_id,
		fo.product_id,
		fo.price,
		fo.customer_id,
		fo.seller_id,
		dio.order_status,
		dio.order_estimated_delivery_date,
		dio.order_delivered_customer_date,
		fp.payment_type,
		fp.payment_value,
		fp.logical_date
from dds.fact_orders fo
join dds.dim_orders dio
on fo.order_id = dio.order_id
join dds.fact_payments fp
on fo.order_id = fp.order_id
join dds.dim_products dip
on fo.product_id = dip.product_id
join dds.dim_customers dic
on fo.customer_id = dic.customer_id
join dds.dim_sellers dis
on fo.seller_id = dis.seller_id;


select sum(fo.price + fo.freight_value )
from dds.fact_orders fo
join dds.dim_orders dio
on fo.order_id = dio.order_id
where fo.logical_date = '2026-10-16 13:59:07.000';