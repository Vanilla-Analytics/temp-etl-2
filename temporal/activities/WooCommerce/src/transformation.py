from datetime import datetime, timezone
from decimal import Decimal
import json

def parse_datetime(dt_str, with_microseconds=False):
    """Parse datetime strings to proper format."""
    if not dt_str:
        return None
    try:
        if 'Z' in dt_str:
            dt_str = dt_str.replace('Z', '+00:00')
        dt = datetime.fromisoformat(dt_str)
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    except (ValueError, TypeError):
        return None

def transform_woo_for_clickhouse(orders_data, connection_id):
    """
    Transform WooCommerce Orders data for ClickHouse insertion.
    """
    prepared_data = []
    batchedAt = datetime.now(timezone.utc)
    
    for order in orders_data:
        # Format boolean fields
        def to_uint8(value):
            if isinstance(value, bool):
                return 1 if value else 0
            if isinstance(value, str):
                return 1 if value.lower() in ['true', 'yes', '1'] else 0
            return 1 if value else 0
        
        # Convert nested objects to JSON strings
        def to_json(value):
            try:
                return json.dumps(value) if value else '{}'
            except:
                return '{}'
        
        # Convert arrays to JSON strings
        def to_json_array(value):
            try:
                return json.dumps(value) if value else '[]'
            except:
                return '[]'
        
        # Handle numeric values safely
        def safe_decimal(value, default='0.00'):
            try:
                return Decimal(str(value)) if value is not None else Decimal(default)
            except:
                return Decimal(default)
        
        row = {
            'id': int(order.get('id', 0)),
            'connected_id': str(connection_id),
            'parent_id': int(order.get('parent_id', 0)),
            'number': str(order.get('number', '')),
            'order_key': str(order.get('order_key', '')),
            'status': str(order.get('status', '')),
            'currency': str(order.get('currency', '')),
            'version': str(order.get('version', '')),
            'prices_include_tax': to_uint8(order.get('prices_include_tax')),
            'date_created': parse_datetime(order.get('date_created')),
            'date_modified': parse_datetime(order.get('date_modified')),
            'discount_total': safe_decimal(order.get('discount_total')),
            'discount_tax': safe_decimal(order.get('discount_tax')),
            'shipping_total': safe_decimal(order.get('shipping_total')),
            'shipping_tax': safe_decimal(order.get('shipping_tax')),
            'cart_tax': safe_decimal(order.get('cart_tax')),
            'total': safe_decimal(order.get('total')),
            'total_tax': safe_decimal(order.get('total_tax')),
            'customer_id': int(order.get('customer_id', 0)),
            'customer_ip_address': str(order.get('customer_ip_address', '')),
            'customer_user_agent': str(order.get('customer_user_agent', '')),
            'payment_method': str(order.get('payment_method', '')),
            'payment_method_title': str(order.get('payment_method_title', '')),
            'transaction_id': str(order.get('transaction_id', '')),
            'customer_note': str(order.get('customer_note', '')),
            'billing_json': to_json(order.get('billing')),
            'shipping_json': to_json(order.get('shipping')),
            'line_items_json': to_json_array(order.get('line_items')),
            'shipping_lines_json': to_json_array(order.get('shipping_lines')),
            'tax_lines_json': to_json_array(order.get('tax_lines')),
            'fee_lines_json': to_json_array(order.get('fee_lines')),
            'coupon_lines_json': to_json_array(order.get('coupon_lines')),
            'meta_data_json': to_json_array(order.get('meta_data')),
            'batchedAt': batchedAt
        }
        prepared_data.append(row)
    
    # Define column order matching the table schema
    column_names = [
        "id", "connected_id", "parent_id", "number", "order_key", "status", 
        "currency", "version", "prices_include_tax", "date_created", 
        "date_modified", "discount_total", "discount_tax", "shipping_total", 
        "shipping_tax", "cart_tax", "total", "total_tax", "customer_id", 
        "customer_ip_address", "customer_user_agent", "payment_method", 
        "payment_method_title", "transaction_id", "customer_note", 
        "billing_json", "shipping_json", "line_items_json", 
        "shipping_lines_json", "tax_lines_json", "fee_lines_json", 
        "coupon_lines_json", "meta_data_json", "batchedAt"
    ]
    
    # Order the data according to column names
    ordered_data = []
    for row in prepared_data:
        ordered_row = [row[col] for col in column_names]
        ordered_data.append(ordered_row)
    
    return ordered_data, column_names, batchedAt.isoformat()