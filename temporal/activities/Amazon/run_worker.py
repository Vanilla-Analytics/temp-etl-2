from temporalio import workflow, activity
from temporalio.client import Client
from temporalio.worker import Worker
import asyncio
import os
import sentry_sdk
import logging
from .workflows import AmazonETLWorkflow
from .activities import amazon
from .interceptor import SentryInterceptor

'''
@docs
Sentry Interceptor Implementation - https://github.com/temporalio/samples-python/blob/2cb0fdde7ede72edf4b40cf8b5440158d9d2234e/sentry/worker.py

'''

async def main():
    sentry_sdk.init(
    dsn="https://d481b0142ed67da2de56d4b6dde84833@o4507200177307648.ingest.us.sentry.io/4509149419995136",
    # Add data like request headers and IP for users,
    # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
    send_default_pii=True,
    )
    api_key = os.environ.get("TEMPORAL_API_KEY")
    logging.info("Starting worker")
    client = await Client.connect(
    "us-west1.gcp.api.temporal.io:7233",
    namespace="grippi-etl.bnvm9",
    rpc_metadata={"temporal-namespace": "grippi-etl.bnvm9"},
    api_key=api_key,
    tls=True,
    )
    workflows = [AmazonETLWorkflow]
    activities = [amazon]
    # Run the worker
    worker = Worker(
        client, task_queue="etl-workflow-queue", workflows=workflows, activities=activities, interceptors=[SentryInterceptor()]
    )
    logging.info("Worker started")
    await worker.run()
    logging.info("Worker stopped")


if __name__ == "__main__":
    asyncio.run(main())
