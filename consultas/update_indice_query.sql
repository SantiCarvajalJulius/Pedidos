UPDATE `{tabla_temp}` 
SET indice = CONCAT(amazon_order_id, merchant_order_id, sku, asin);
