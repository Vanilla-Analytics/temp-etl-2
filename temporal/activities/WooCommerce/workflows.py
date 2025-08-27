from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
import logging
from dataclasses import dataclass
from typing import Union, List, Any, Optional
from datetime import datetime

@dataclass
class WooCommercePayload:
    connected_id: str
    base_url: str
    consumer_key: str
    consumer_secret: str
    last_run_ts: Optional[datetime] = None
    fill_type: Optional[str] = None

@workflow.defn
class WooCommerceETLWorkflow:
    @workflow.run
    async def run(self, payload: WooCommercePayload):
        logger = workflow.logger
        logger.info(f"WooCommerceETLWorkflow started for: {payload.connected_id}")
        
        try:
            logger.info("Executing WooCommerce activity")
            
            # Import activity here to avoid circular imports
            with workflow.unsafe.imports_passed_through():
                from temporal.activities.WooCommerce.activities import woocommerce
            
            result = await workflow.execute_activity(
                woocommerce, 
                payload, 
                retry_policy=RetryPolicy(maximum_attempts=2),
                schedule_to_close_timeout=timedelta(hours=3),
                start_to_close_timeout=timedelta(minutes=10)
            )
            
            logger.info(f"WooCommerce activity completed successfully")
            return {"status": "success", "code": 200, "message": "WooCommerce ETL completed", "result": result}
        
        except Exception as e:
            logger.error(f"WooCommerce activity failed: {str(e)}")
            return {"status": "failure", "code": 500, "message": f"WooCommerce ETL failed: {str(e)}"}