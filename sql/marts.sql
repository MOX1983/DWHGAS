create schema marts;


create materialized view marts.mv_orders_items_payments as 
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



refresh materialized view marts.mv_orders_items_payments;

drop view if exists marts.v_orders_items_payments;

create view marts.v_orders_items_payments as 
select * from marts.mv_orders_items_payments;



select sum(fo.price + fo.freight_value)
from dds.fact_orders fo
join dds.dim_products dp
on fo.product_id = dp.product_id and fo.logical_date = dp.logical_date
join dds.fact_payments fp 
on fo.order_id = fp.order_id and fo.logical_date = fp.logical_date
where fo.logical_date = '2026-10-20 14:14:48.000';


select count(*)
from dds.fact_orders fo
join dds.dim_products dp
on fo.product_id = dp.product_id and fo.logical_date = dp.logical_date
where fo.logical_date = '2026-10-20 14:14:48.000';


select count(*)
from dds.fact_orders fo 
where fo.logical_date = '2026-10-20 14:14:48.000';









