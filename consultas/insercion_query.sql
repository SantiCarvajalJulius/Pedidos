LOAD DATA LOW_PRIORITY LOCAL INFILE '{ruta_completa_mysql}'
REPLACE INTO TABLE `{tabla_temp}`
CHARACTER SET utf8mb4
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    amazon_order_id, merchant_order_id, purchase_date, last_updated_date, order_status, 
    fulfillment_channel, sales_channel, order_channel, url, ship_service_level, product_name, 
    sku, asin, number_of_items, item_status, tax_collection_model, tax_collection_responsible_party, 
    quantity, currency, item_price, item_tax, shipping_price, shipping_tax, gift_wrap_price, 
    gift_wrap_tax, item_promotion_discount, ship_promotion_discount, ship_city, ship_state, 
    ship_postal_code, ship_country, promotion_ids, payment_method_details, item_extensions_data, 
    is_business_order, purchase_order_number, price_designation, fulfilled_by, buyer_company_name, 
    buyer_cst_number, buyer_vat_number, buyer_tax_registration_id, buyer_tax_registration_country, 
    buyer_tax_registration_type, customized_url, customized_page, is_heavy_or_bulky, 
    is_replacement_order, is_exchange_order, original_order_id, is_amazon_invoiced, 
    vat_exclusive_item_price, vat_exclusive_shipping_price, vat_exclusive_giftwrap_price, 
    licensee_name, license_number, license_state, license_expiration_date, is_iba, is_transparency, 
    default_ship_from_address_name, default_ship_from_address_field_1, default_ship_from_address_field_2, 
    default_ship_from_address_field_3, default_ship_from_city, default_ship_from_state, 
    default_ship_from_country, default_ship_from_postal_code, is_ispu_order, store_chain_store_id, 
    is_pickup_point_order, pickup_point_type, is_buyer_requested_cancellation, 
    buyer_requested_cancel_reason, fecha_add
);
