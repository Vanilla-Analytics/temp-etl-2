from datetime import datetime, timezone
from decimal import Decimal
import json

def parse_datetime(dt_str, with_microseconds=False):
    """Parse datetime strings to proper format."""
    if not dt_str:
        return None
    if with_microseconds:
        # For DateTime64(3) fields that need millisecond precision
        if 'Z' in dt_str:
            # Handle ISO format with Z
            dt_str = dt_str.replace('Z', '+00:00')
        return datetime.fromisoformat(dt_str)
    else:
        # For regular DateTime fields
        try:
            return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            # Some timestamps might have milliseconds
            if 'Z' in dt_str:
                dt_str = dt_str.replace('Z', '+00:00')
            return datetime.fromisoformat(dt_str)

def transform_amazon_for_clickhouse(orders_data, connection_id):
    """
    Transform Amazon Orders data for ClickHouse insertion.
    
    Args:
        orders_data: List of Amazon order dictionaries
        connection_id: The unique identifier for the connection
        
    Returns:
        Tuple of (ordered_data, column_names, batchedAt)
    """
    prepared_data = []
    # Make it UTC time
    batchedAt = datetime.now(timezone.utc).isoformat()
    
    for order in orders_data:
        # Extract OrderTotal data or set defaults
        order_total = None
        order_total_currency_code = ""
        if 'OrderTotal' in order and order['OrderTotal']:
            try:
                order_total = Decimal(order['OrderTotal'].get('Amount', '0.00'))
                order_total_currency_code = order['OrderTotal'].get('CurrencyCode', '')
            except (TypeError, ValueError):
                order_total = None
        
        # Extract BuyerEmail or set default
        buyer_email = None
        if 'BuyerInfo' in order and order['BuyerInfo'] and 'BuyerEmail' in order['BuyerInfo']:
            buyer_email = order['BuyerInfo'].get('BuyerEmail')
        
        # Convert ShippingAddress to JSON string or set default
        shipping_address = '{}'
        if 'ShippingAddress' in order and order['ShippingAddress']:
            shipping_address = json.dumps(order['ShippingAddress'])
        
        # Convert OrderItems to JSON string or set default
        order_items = '[]'
        if 'OrderItems' in order and order['OrderItems']:
            order_items = json.dumps(order['OrderItems'])
        
        # Format boolean fields (convert to 0/1)
        def to_uint8(value):
            if isinstance(value, bool):
                return 1 if value else 0
            if isinstance(value, str):
                return 1 if value.lower() == 'true' else 0
            return 0
        
        # Format the row according to the table schema
        row = {
            'AmazonOrderId': order.get('AmazonOrderId', ''),
            'connected_id': connection_id,
            'PurchaseDate': parse_datetime(order.get('PurchaseDate'), with_microseconds=True),
            'LastUpdateDate': parse_datetime(order.get('LastUpdateDate'), with_microseconds=True),
            'OrderStatus': order.get('OrderStatus', ''),
            'OrderType': order.get('OrderType', ''),
            'FulfillmentChannel': order.get('FulfillmentChannel', ''),
            'SalesChannel': order.get('SalesChannel', ''),
            'ShipServiceLevel': order.get('ShipServiceLevel', ''),
            'ShipmentServiceLevelCategory': order.get('ShipmentServiceLevelCategory', ''),
            'EarliestShipDate': parse_datetime(order.get('EarliestShipDate'), with_microseconds=True),
            'LatestShipDate': parse_datetime(order.get('LatestShipDate'), with_microseconds=True),
            'NumberOfItemsShipped': int(order.get('NumberOfItemsShipped', 0)),
            'NumberOfItemsUnshipped': int(order.get('NumberOfItemsUnshipped', 0)),
            'PaymentMethod': order.get('PaymentMethod', ''),
            'PaymentMethodDetails': order.get('PaymentMethodDetails', []),
            'MarketplaceId': order.get('MarketplaceId', ''),
            'SellerOrderId': order.get('SellerOrderId', ''),
            'IsPremiumOrder': to_uint8(order.get('IsPremiumOrder')),
            'IsPrime': to_uint8(order.get('IsPrime')),
            'IsBusinessOrder': to_uint8(order.get('IsBusinessOrder')),
            'IsReplacementOrder': to_uint8(order.get('IsReplacementOrder')),
            'IsGlobalExpressEnabled': to_uint8(order.get('IsGlobalExpressEnabled')),
            'HasRegulatedItems': to_uint8(order.get('HasRegulatedItems')),
            'IsISPU': to_uint8(order.get('IsISPU')),
            'IsAccessPointOrder': to_uint8(order.get('IsAccessPointOrder')),
            'IsSoldByAB': to_uint8(order.get('IsSoldByAB')),
            'OrderTotal': order_total,
            'OrderTotalCurrencyCode': order_total_currency_code,
            'BuyerEmail': buyer_email,
            'ShippingAddress': shipping_address,
            'OrderItems': order_items,
            'batchedAt': parse_datetime(batchedAt, with_microseconds=True)
        }
        prepared_data.append(row)
    
    # Define column order matching the table schema
    column_names = [
        "AmazonOrderId", "connected_id", "PurchaseDate", "LastUpdateDate", 
        "OrderStatus", "OrderType", "FulfillmentChannel", "SalesChannel", 
        "ShipServiceLevel", "ShipmentServiceLevelCategory", "EarliestShipDate", 
        "LatestShipDate", "NumberOfItemsShipped", "NumberOfItemsUnshipped", 
        "PaymentMethod", "PaymentMethodDetails", "MarketplaceId", "SellerOrderId", 
        "IsPremiumOrder", "IsPrime", "IsBusinessOrder", "IsReplacementOrder", 
        "IsGlobalExpressEnabled", "HasRegulatedItems", "IsISPU", "IsAccessPointOrder", 
        "IsSoldByAB", "OrderTotal", "OrderTotalCurrencyCode", "BuyerEmail", 
        "ShippingAddress", "OrderItems", "batchedAt"
    ]
    
    # Order the data according to column names
    ordered_data = []
    for row in prepared_data:
        ordered_row = [row[col] for col in column_names]
        ordered_data.append(ordered_row)
    
    return ordered_data, column_names, batchedAt

def process_amazon_orders(raw_data, connection_id):
    """
    Process raw Amazon Orders API response and transform for ClickHouse.
    
    Args:
        raw_data: Raw JSON response from Amazon API
        connection_id: The unique identifier for the connection
        
    Returns:
        Tuple of (ordered_data, column_names, batchedAt)
    """
    # Extract orders from the payload
    if isinstance(raw_data, str):
        data = json.loads(raw_data)
    else:
        data = raw_data
    
    # Navigate to the Orders list in the payload
    orders = data.get('payload', {}).get('Orders', [])
    
    # Transform the data for ClickHouse
    return transform_amazon_for_clickhouse(orders, connection_id)