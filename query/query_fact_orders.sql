--- fact_orders

select a.user_id, a.order_id, b.product_id, c.product_name, b.order_item_quantity, order_date, d.payment_name, order_price, order_discount + e.voucher_price as price_discount, order_total, e.voucher_name
from tb_orders a
left join tb_order_items b on a.order_id = b.order_id 
left join tb_products c on b.product_id = c.product_id 
left join tb_payments d ON a.payment_id = d.payment_id
left join tb_vouchers e ON a.voucher_id = e.voucher_id

order by order_date desc