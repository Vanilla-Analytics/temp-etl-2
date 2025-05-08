import json

import json
from datetime import datetime
from decimal import Decimal
from clickhouse_connect import create_async_client
from typing import List
import asyncio
from datetime import timezone
# Sample data from your paste.txt file

# Helper function to convert datetime strings to proper format
def parse_datetime(dt_str, with_microseconds=False):
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
        return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%SZ')

async def test_async_insert(master_orders):
    # Connection parameters - update these with your actual values

    orders_data = master_orders.copy()
    connection_id = "sereko.myshopify.com"
    # Create the async client with async_insert settings
    client = create_async_client(
        host='sv26l0r615.us-central1.gcp.clickhouse.cloud',
        user='default',
        password='ccnm21v1_J5xE',
        secure=True,
        settings={
            'async_insert': 1,
            'wait_for_async_insert': 1,
            'async_insert_timeout': 10000,
            'async_insert_max_data_size': 1000000,
            'async_insert_busy_timeout': 200
        }
    )
    
    # Prepare the data
    prepared_data = []
    for order in orders_data:
        # Convert lineItems to a JSON string
        line_items_json = json.dumps(order.get('lineItems', []))
        
        # Format the row according to the table schema
        # TODO: Add connection_id to be dynamic for all the client orders
        row = {
            'id': order.get('id', ''),
            'connection_id': "sereko.myshopify.com",
            'name': order.get('name', ''),
            'createdAt': parse_datetime(order.get('createdAt')),
            'updatedAt': parse_datetime(order.get('updatedAt'), with_microseconds=True),
            'cancelledAt': parse_datetime(order.get('cancelledAt')),
            'processedAt': parse_datetime(order.get('processedAt')),
            'cancelReason': order.get('cancelReason'),
            'displayFinancialStatus': order.get('displayFinancialStatus', ''),
            'displayFulfillmentStatus': order.get('displayFulfillmentStatus', ''),
            'fullyPaid': 1 if order.get('fullyPaid') else 0,
            'unpaid': 1 if order.get('unpaid') else 0,
            'test': 1 if order.get('test') else 0,
            'currencyCode': order.get('currencyCode', ''),
            'taxesIncluded': 1 if order.get('taxesIncluded') else 0,
            'taxExempt': 1 if order.get('taxExempt') else 0,
            'dutiesIncluded': 1 if order.get('dutiesIncluded') else 0,
            'discountCode': order.get('discountCode'),
            'currentCartDiscountAmountSet': Decimal(str(order.get('currentCartDiscountAmountSet', 0))),
            'currentShippingPriceSet': Decimal(str(order.get('currentShippingPriceSet', 0))),
            'currentSubtotalPriceSet': Decimal(str(order.get('currentSubtotalPriceSet', 0))),
            'currentTotalDiscountsSet': Decimal(str(order.get('currentTotalDiscountsSet', 0))),
            'currentTotalTaxSet': Decimal(str(order.get('currentTotalTaxSet', 0))),
            'currentTotalPriceSet': Decimal(str(order.get('currentTotalPriceSet', 0))),
            'netPaymentSet': Decimal(str(order.get('netPaymentSet', 0))),
            'currentTotalDutiesSet': Decimal(str(order.get('currentTotalDutiesSet', 0))) if order.get('currentTotalDutiesSet') is not None else None,
            'currentTotalAdditionalFeesSet': Decimal(str(order.get('currentTotalAdditionalFeesSet', 0))) if order.get('currentTotalAdditionalFeesSet') is not None else None,
            'totalDiscountsSet': Decimal(str(order.get('totalDiscountsSet', 0))),
            'totalPriceSet': Decimal(str(order.get('totalPriceSet', 0))),
            'totalReceivedSet': Decimal(str(order.get('totalReceivedSet', 0))),
            'totalRefundedSet': Decimal(str(order.get('totalRefundedSet', 0))),
            'totalShippingPriceSet': Decimal(str(order.get('totalShippingPriceSet', 0))),
            'totalTaxSet': Decimal(str(order.get('totalTaxSet', 0))),
            'totalTipReceivedSet': Decimal(str(order.get('totalTipReceivedSet', 0))),
            'currentSubtotalLineItemsQuantity': int(order.get('currentSubtotalLineItemsQuantity', 0)),
            'lineItems': line_items_json,
            'paymentGatewayNames': order.get('paymentGatewayNames', []),
            'tags': order.get('tags', []),
            'note': order.get('note')
        }
        prepared_data.append(row)
    
    try:
        # Initialize insert timer
        start_time = datetime.now()
        print(f"Starting async insert of {len(prepared_data)} records at {start_time}")
        
        # Perform the async insert
        result = await client.insert('temp_shopify_orders', prepared_data)
        print(result)

        # Calculate elapsed time
        elapsed = datetime.now() - start_time
        print(f"Async insert initiated in {elapsed.total_seconds():.3f} seconds")
        print(f"Insert initiated for {len(prepared_data)} records")
        
        # Optional: If you want to verify the insert was successful
        # Wait a moment for async inserts to be processed
        
        # Count the records to verify
        query = f"SELECT count() FROM temp_shopify_orders WHERE connection_id = '{connection_id}'"
        count_result = await client.query_arrow(query)
        count = count_result.named_columns()['count()'][0]
        print(f"Verified count after insert: {count}")
        
    except Exception as e:
        print(f"Error during async insert: {e}")
    finally:
        # Close the client
        await client.close()
        print("Test completed")
        return True

def coerce_order_data(order_data):
    """
    Coerce order data using explicit sequential transformations with get() methods
    for better error handling and future expandability.
    """
    result = {}
    
    # Helper functions for common transformations
    def extract_money_value(money_dict, default=0):
        if not money_dict:
            return default
        shop_money = money_dict.get('shopMoney', {})
        amount_str = shop_money.get('amount')
        return float(amount_str) if amount_str is not None else default
    
    def safe_str(value, default=None):
        return str(value) if value is not None else default
    
    def safe_bool(value, default=False):
        return bool(value) if value is not None else default
    
    def safe_int(value, default=0):
        return int(value) if value is not None else default
    
    def safe_float(value, default=0.0):
        return float(value) if value is not None else default
    
    def safe_list(value, default=None):
        if default is None:
            default = []
        return list(value) if value is not None else default
    
    # Process basic string fields
    for field in ['id', 'name', 'createdAt', 'updatedAt', 'currencyCode', 
                 'displayFinancialStatus', 'displayFulfillmentStatus', 'processedAt']:
        result[field] = safe_str(order_data.get(field))
    
    # Process nullable string fields
    for field in ['cancelledAt', 'cancelReason', 'note', 'discountCode']:
        result[field] = safe_str(order_data.get(field), None)
    
    # Process boolean fields
    for field in ['dutiesIncluded', 'fullyPaid', 'taxesIncluded', 'taxExempt', 'unpaid', 'test']:
        result[field] = safe_bool(order_data.get(field))
    
    # Process integer fields
    result['currentSubtotalLineItemsQuantity'] = safe_int(order_data.get('currentSubtotalLineItemsQuantity'))
    
    # Process money fields
    money_fields = [
        'totalDiscountsSet', 'totalPriceSet', 'totalReceivedSet', 'totalRefundedSet',
        'totalShippingPriceSet', 'totalTaxSet', 'totalTipReceivedSet', 'currentCartDiscountAmountSet',
        'currentShippingPriceSet', 'currentSubtotalPriceSet', 'currentTotalDiscountsSet',
        'currentTotalTaxSet', 'currentTotalPriceSet', 'netPaymentSet'
    ]
    
    # Handle special cases for nullable money fields
    nullable_money_fields = ['currentTotalAdditionalFeesSet', 'currentTotalDutiesSet']
    
    for field in money_fields:
        result[field] = extract_money_value(order_data.get(field), 0.0)
    
    for field in nullable_money_fields:
        money_dict = order_data.get(field)
        result[field] = extract_money_value(money_dict, None) if money_dict is not None else None
    
    # Process line items
    line_items = []
    line_items_data = order_data.get('lineItems', {}).get('edges', [])
    
    for edge in line_items_data:
        node = edge.get('node', {})
        if not node:
            continue
            
        item = {
            'id': safe_str(node.get('id')),
            'name': safe_str(node.get('name')),
            'sku': safe_str(node.get('sku')),
            'title': safe_str(node.get('title')),
            'quantity': safe_int(node.get('quantity')),
            'currentQuantity': safe_int(node.get('currentQuantity')),
            'variantTitle': safe_str(node.get('variantTitle'), None),
            'originalTotalSet': extract_money_value(node.get('originalTotalSet')),
            'originalUnitPriceSet': extract_money_value(node.get('originalUnitPriceSet')),
            'totalDiscountSet': extract_money_value(node.get('totalDiscountSet')),
            'discountedUnitPriceAfterAllDiscountsSet': extract_money_value(node.get('discountedUnitPriceAfterAllDiscountsSet'))
        }
        line_items.append(item)
    
    result['lineItems'] = line_items
    
    # Process array fields
    result['paymentGatewayNames'] = safe_list(order_data.get('paymentGatewayNames'))
    result['tags'] = safe_list(order_data.get('tags'))
    
    return result

def transform_for_clickhouse(master_orders,connection_id):
    #TODO: Add connection_id to be dynamic for all the client orders
    prepared_data = []
    # Make it UTC time
    batchedAt = datetime.now(timezone.utc).isoformat()
    for order in master_orders:
        # Convert lineItems to a JSON string
        line_items_json = json.dumps(order.get('lineItems', []))
        customer_json = json.dumps([])
        refund_json = json.dumps([])
        # Format the row according to the table schema
        # TODO: Add connection_id to be dynamic for all the client orders
        row = {
            'id': order.get('id', ''),
            'connection_id': connection_id,
            'name': order.get('name', ''),
            'createdAt': parse_datetime(order.get('createdAt'),with_microseconds=True),
            'updatedAt': parse_datetime(order.get('updatedAt'), with_microseconds=True),
            'cancelledAt': parse_datetime(order.get('cancelledAt'),with_microseconds=True),
            'processedAt': parse_datetime(order.get('processedAt'),with_microseconds=True),
            'cancelReason': order.get('cancelReason'),
            'displayFinancialStatus': order.get('displayFinancialStatus', ''),
            'displayFulfillmentStatus': order.get('displayFulfillmentStatus', ''),
            'fullyPaid': 1 if order.get('fullyPaid') else 0,
            'unpaid': 1 if order.get('unpaid') else 0,
            'test': 1 if order.get('test') else 0,
            'currencyCode': order.get('currencyCode', ''),
            'taxesIncluded': 1 if order.get('taxesIncluded') else 0,
            'taxExempt': 1 if order.get('taxExempt') else 0,
            'dutiesIncluded': 1 if order.get('dutiesIncluded') else 0,
            'discountCode': order.get('discountCode'),
            'currentCartDiscountAmountSet': Decimal(str(order.get('currentCartDiscountAmountSet', 0))),
            'currentShippingPriceSet': Decimal(str(order.get('currentShippingPriceSet', 0))),
            'currentSubtotalPriceSet': Decimal(str(order.get('currentSubtotalPriceSet', 0))),
            'currentTotalDiscountsSet': Decimal(str(order.get('currentTotalDiscountsSet', 0))),
            'currentTotalTaxSet': Decimal(str(order.get('currentTotalTaxSet', 0))),
            'currentTotalPriceSet': Decimal(str(order.get('currentTotalPriceSet', 0))),
            'netPaymentSet': Decimal(str(order.get('netPaymentSet', 0))),
            'currentTotalDutiesSet': Decimal(str(order.get('currentTotalDutiesSet', 0))) if order.get('currentTotalDutiesSet') is not None else None,
            'currentTotalAdditionalFeesSet': Decimal(str(order.get('currentTotalAdditionalFeesSet', 0))) if order.get('currentTotalAdditionalFeesSet') is not None else None,
            'totalDiscountsSet': Decimal(str(order.get('totalDiscountsSet', 0))),
            'totalPriceSet': Decimal(str(order.get('totalPriceSet', 0))),
            'totalReceivedSet': Decimal(str(order.get('totalReceivedSet', 0))),
            'totalRefundedSet': Decimal(str(order.get('totalRefundedSet', 0))),
            'totalShippingPriceSet': Decimal(str(order.get('totalShippingPriceSet', 0))),
            'totalTaxSet': Decimal(str(order.get('totalTaxSet', 0))),
            'totalTipReceivedSet': Decimal(str(order.get('totalTipReceivedSet', 0))),
            'currentSubtotalLineItemsQuantity': int(order.get('currentSubtotalLineItemsQuantity', 0)),
            'lineItems': line_items_json,
            'paymentGatewayNames': order.get('paymentGatewayNames', []),
            'tags': order.get('tags', []),
            'note': order.get('note'),
            'customer': customer_json,
            'refunds': refund_json,
            'batchedAt': parse_datetime(batchedAt,with_microseconds=True)   
        }
        prepared_data.append(row)
    column_names = [
    "id", "connection_id", "name", "createdAt", "updatedAt", "cancelledAt", 
    "processedAt", "cancelReason", "displayFinancialStatus", "displayFulfillmentStatus", 
    "fullyPaid", "unpaid", "test", "currencyCode", "taxesIncluded", "taxExempt", 
    "dutiesIncluded", "discountCode", "currentCartDiscountAmountSet", "currentShippingPriceSet", 
    "currentSubtotalPriceSet", "currentTotalDiscountsSet", "currentTotalTaxSet", 
    "currentTotalPriceSet", "netPaymentSet", "currentTotalDutiesSet", 
    "currentTotalAdditionalFeesSet", "totalDiscountsSet", "totalPriceSet", 
    "totalReceivedSet", "totalRefundedSet", "totalShippingPriceSet", "totalTaxSet", 
    "totalTipReceivedSet", "currentSubtotalLineItemsQuantity", "lineItems", 
    "paymentGatewayNames", "tags", "note", "customer", "refunds", "batchedAt"
    ]
    ordered_data = []
    for row in prepared_data:
        ordered_row = [row[col] for col in column_names]
        ordered_data.append(ordered_row)
    return ordered_data,column_names,batchedAt



if __name__=="__main__":
    orders = None
    with open("orders.json", 'r') as f:
        orders =  json.load(f)
    master_orders = []
    for order in orders:
        master_orders.append(coerce_order_data(order))
    print(master_orders[0:5])
    data = asyncio.run(test_async_insert(master_orders))
    print(data)

