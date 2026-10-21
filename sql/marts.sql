create schema marts;


drop view marts.orders_items_payments;
create view marts.orders_items_payments as 
select fo.order_id,
		fo.order_item_id,
		dp.product_id,
		dp.product_category_name,
		fo.price,
		fo.freight_value,
		fo.customer_id,
		fo.seller_id,
		fo.logical_date
from dds.fact_orders fo
join dds.dim_products dp
on fo.product_id = dp.product_id and fo.logical_date = dp.logical_date;


select sum(fo.price + fo.freight_value)
from dds.fact_orders fo
join dds.dim_products dp
on fo.product_id = dp.product_id and fo.logical_date = dp.logical_date
where fo.logical_date = '2026-10-20 14:14:48.000';


select count(*)
from dds.fact_orders fo
join dds.dim_products dp
on fo.product_id = dp.product_id and fo.logical_date = dp.logical_date
where fo.logical_date = '2026-10-20 14:14:48.000';











