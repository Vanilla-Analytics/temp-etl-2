from datetime import timedelta
from temporalio import workflow
from .activities import amazon, AccountPayload
from temporalio.common import RetryPolicy
import logging
# Always coordinate stuff between the workflows and activities

# Think about what data formats are best for the workflow and activities

@workflow.defn
class AmazonETLWorkflow:
    @workflow.run
    async def run(self, request: AccountPayload):
        logger = workflow.logger
        logger.info(f"AmazonETLWorkflow started for account: {request.connected_id}")
        try:
            logger.info("Executing amazon activity")
            val = await workflow.execute_activity(
                amazon, request, retry_policy=RetryPolicy(maximum_attempts=2),schedule_to_close_timeout=timedelta(hours=3)
            )
            logger.info(f"Amazon activity completed successfully with result: {val}")
            return {"status": "success", "code": 200, "message": "ETL workflow completed successfully"}
        except Exception as e:
            logger.error(f"Amazon activity failed: {str(e)}")
            return {"status": "failure", "code": 500, "message": f"ETL workflow failed: {str(e)}"}
        
