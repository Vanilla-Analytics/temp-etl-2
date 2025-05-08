from temporalio import activity
import asyncio
import random
import asyncio
from datetime import datetime
from dataclasses import dataclass
from typing import Union, List, Optional, Any
import logging

@dataclass
class DataExtractionRequest:
    account_id: str
    start_date: str
    end_date: str

@dataclass
class AccountPayload:
    connected_id: str
    id: Union[int, List[int], Any]
    connected_name: Union[str, List[str], Any]
    platform: Union[str, List[str], Any]
    manager_id: Union[str, List[str], Any]
    access_token: Union[str, List[str], Any]
    refresh_token: Union[str, List[str], Any]
    timezone: Union[str, List[str], Any]
    workspace_id: Union[int, List[int], Any]
    grippi_workspace_id: Union[str, List[str], Any]
    clerk_org_id: Union[str, List[str], Any]
    clerk_org_slug: Union[str, List[str], Any]
    status: Union[str, List[str], Any]
    last_run_ts: Union[datetime, List[datetime], Any]
    client_secret: str
    region: str

@activity.defn
async def amazon(request: AccountPayload):
    try:
        activity.logger.info(f"Within acitivities.py - Data extraction activity executed for account")

        #TODO: Check postgres for last run ts
        #Just use the start of this month as the last run ts
        '''
        1. Check Postgres for the last run ts
        2. If the last run ts is available and is less than 2 months old, use it as the start time
        3. If the last run ts is not available or is older than 2 months, start from the start of last month as long as its not older than 60 days
        4. Start the data pull from shopify api with all the errors handled
        5. Apply the transformations as required with any possible bugs cropping up
        6. Transform the data to the required format for clickhouse
        7. Perform an async insert into clickhouse
        8. On recieving a confirmation from clickhouse, update the last order created_ts as the last run ts in postgres
        '''
        # Import non-deterministic libraries only within the activity
        from .src.extraction import AmazonClient
        from .src.transformation import coerce_order_data, transform_for_clickhouse
        from .src.loading import main
        from .src.types import AmazonOrderRequest
        import json

        last_run_ts = request.last_run_ts
        fill_type = None
        if last_run_ts is None:
            fill_type = "backfill"
            last_run_ts = datetime(datetime.now().year, datetime.now().month - 1, 1)
        else:
            fill_type = "incremental"
            last_run_ts = last_run_ts
            print(last_run_ts)
            # Convert last_run_ts to datetime object - 2025-04-06 18:27:10
            last_run_ts = datetime.strptime(last_run_ts, "%Y-%m-%d %H:%M:%S")
        shop_name = request.connected_id
        access_token = request.access_token
        start_time = last_run_ts
        # Extraction.py utilization
        activity.logger.info(f"Data extracted for account {shop_name} with fill type {fill_type} and start time {start_time}")
        client = AmazonClient(request=AmazonOrderRequest(refresh_token=request.refresh_token,client_secret=request.client_secret,region=request.region,fill_type=fill_type,CreatedAfter=start_time,LastUpdatedAfter=None,MaxResultsPerPage=100))
        orders = await client.get_orders() 
        activity.logger.info(f"Orders extracted for account {shop_name}")
        activity.logger.info(f"Orders: {len(orders)}")
        # Transformation Utilization
        master_orders = []
        for order in orders:
            master_orders.append(coerce_order_data(order))
        activity.logger.info(f"Master orders: {len(master_orders)}")
        if len(master_orders) > 0:
            ordered_data, column_names, batchedAt = transform_for_clickhouse(master_orders, shop_name)
        # Loading.py utilization
            result = await main(table_name="aa_master_shopify_orders", data=ordered_data, column_names=column_names, connection_id=shop_name, batchedAt=batchedAt)
            activity.logger.info(f"Data loaded for account {shop_name}")
            return result
        else:
            activity.logger.info(f"No orders found for account {shop_name} with fill type {fill_type} and start time {start_time}")
            activity.logger.info(f"No data loaded for account {shop_name}")
            return None
    except Exception as e:
        activity.logger.error(f"Error in activities.py: {e}")
        return None
    
