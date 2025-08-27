from temporalio import activity
from datetime import datetime
from dataclasses import dataclass
from typing import Optional
import logging
import sys
import os

# Add the current directory to Python path to ensure local imports work
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

@dataclass
class WooCommercePayload:
    connected_id: str
    base_url: str
    consumer_key: str
    consumer_secret: str
    last_run_ts: Optional[datetime] = None
    fill_type: Optional[str] = None

@activity.defn
async def woocommerce(payload: WooCommercePayload):
    try:
        activity.logger.info(f"WooCommerce data extraction activity executed for account {payload.connected_id}")

        # Determine fill type based on last run timestamp
        last_run_ts = payload.last_run_ts
        fill_type = payload.fill_type or "backfill"
        
        if last_run_ts is None:
            fill_type = "backfill"
            # Default to 30 days back if no last_run_ts provided
            from datetime import timedelta
            last_run_ts = datetime.now() - timedelta(days=30)
        
        connected_id = payload.connected_id
        
        # Import WooCommerce modules using absolute imports
        try:
            from temporal.activities.WooCommerce.src.extraction import WooClient
            from temporal.activities.WooCommerce.src.transformation import transform_woo_for_clickhouse
            from temporal.activities.WooCommerce.src.loading import main
            from temporal.activities.WooCommerce.src.types import WooOrderRequest


        except ImportError as e:
            activity.logger.error(f"Import error: {e}")
            # Fallback to relative imports
            from .src.extraction import WooClient
            from .src.transformation import transform_woo_for_clickhouse
            from .src.loading import main
            from .src.types import WooOrderRequest

        # Extraction
        activity.logger.info(f"Data extraction for {connected_id} with fill type {fill_type}")
        
        client = WooClient(request=WooOrderRequest(
            base_url=payload.base_url,
            consumer_key=payload.consumer_key,
            consumer_secret=payload.consumer_secret,
            connected_id=connected_id,
            fill_type=fill_type,
            CreatedAfter=last_run_ts if fill_type == "backfill" else None,
            LastUpdatedAfter=last_run_ts if fill_type == "incremental" else None,
            PerPage=100
        ))
        
        orders = await client.get_orders()
        activity.logger.info(f"Orders extracted: {len(orders)}")

        # Transformation and Loading
        if orders:
            ordered_data, column_names, batchedAt = transform_woo_for_clickhouse(orders, connected_id)
            
            result = await main(
                table_name="aa_master_woo-commerce_orders", 
                data=ordered_data, 
                column_names=column_names, 
                connection_id=connected_id, 
                batchedAt=batchedAt
            )
            
            activity.logger.info(f"Data loaded successfully for {connected_id}")
            return {"orders_processed": len(orders), "status": "success"}
        else:
            activity.logger.info(f"No orders found for {connected_id}")
            return {"orders_processed": 0, "status": "no_data"}
            
    except Exception as e:
        activity.logger.error(f"Error in WooCommerce activity: {e}")
        activity.logger.exception("Detailed traceback:")
        raise