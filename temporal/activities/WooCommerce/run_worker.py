from temporalio.client import Client
from temporalio.worker import Worker
import asyncio
import os
import sentry_sdk
import logging

async def main():
    sentry_sdk.init(
        dsn="https://d481b0142ed67da2de56d4b6dde84833@o4507200177307648.ingest.us.sentry.io/4509149419995136",
        send_default_pii=True,
    )
    
    api_key = os.environ.get("TEMPORAL_API_KEY")
    logging.info("Starting worker with WooCommerce support")
    
    client = await Client.connect(
        "us-west1.gcp.api.temporal.io:7233",
        namespace="grippi-etl.bnvm9",
        rpc_metadata={"temporal-namespace": "grippi-etl.bnvm9"},
        api_key=api_key,
        tls=True,
    )
    
    # Import workflows and activities
    from .workflows import WooCommerceETLWorkflow
    from .activities import woocommerce
    from .interceptor import SentryInterceptor
    
    # Register both workflows and activities
    workflows = [WooCommerceETLWorkflow]
    activities = [woocommerce]
    
    # Run the worker
    worker = Worker(
        client, 
        task_queue="etl-workflow-queue", 
        workflows=workflows, 
        activities=activities, 
        interceptors=[SentryInterceptor()]
    )
    
    logging.info("Worker started with WooCommerce support")
    await worker.run()
    logging.info("Worker stopped")

if __name__ == "__main__":
    asyncio.run(main())