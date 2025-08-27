import asyncio
import os
from datetime import datetime, timedelta
from temporalio.client import Client
from temporal.activities.WooCommerce.workflows import WooCommerceETLWorkflow, WooCommercePayload
from temporalio.exceptions import WorkflowAlreadyStartedError

async def main():
    api_key = os.environ.get("TEMPORAL_API_KEY")

    # Connect to Temporal Cloud
    client = await Client.connect(
        "us-west1.gcp.api.temporal.io:7233",
        namespace="grippi-etl.bnvm9",
        rpc_metadata={"temporal-namespace": "grippi-etl.bnvm9"},
        api_key=api_key,
        tls=True,
    )

    print("Connected to Temporal Cloud ✅")

    # Create a unique workflow ID using timestamp
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    workflow_id = f"woocommerce-etl-workflow-{timestamp}"

    try:
        # Create the payload with correct structure
        payload = WooCommercePayload(
            connected_id="shivaloka-store-1",
            base_url="https://shivaloka.co",
            consumer_key="ck_7176a7d48632ba33f0b57d71424b2056fff4dec9",
            consumer_secret="cs_ff1f04ea3c8c75fba01452c1196e9e1a44544859",
            last_run_ts=datetime.now() - timedelta(days=30),  # Default to 30 days back
            fill_type="backfill"
        )

        # Start the workflow
        handle = await client.start_workflow(
            WooCommerceETLWorkflow.run,
            payload,
            id=workflow_id,
            task_queue="etl-workflow-queue",
        )
        print(f"✅ Started workflow {handle.id} with Run ID {handle.run_id}")

    except WorkflowAlreadyStartedError:
        print(f"⚠️ Workflow {workflow_id} already running, terminating and restarting...")

        # Get handle for running workflow
        handle = client.get_workflow_handle(workflow_id)

        # Terminate existing workflow
        await handle.terminate("Restarting workflow")

        # Start new one
        new_handle = await client.start_workflow(
            WooCommerceETLWorkflow.run,
            payload,
            id=workflow_id,
            task_queue="etl-workflow-queue",
        )
        print(f"🔄 Restarted workflow {new_handle.id} with Run ID {new_handle.run_id}")

if __name__ == "__main__":
    asyncio.run(main())