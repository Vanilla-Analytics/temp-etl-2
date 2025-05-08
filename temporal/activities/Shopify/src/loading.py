from clickhouse_connect import create_async_client, get_async_client
import clickhouse_connect
import asyncio
from datetime import datetime
from temporal.activities.Shopify.src.database import SupabaseDatabase
import os
import logging
async def main(table_name,data,column_names,connection_id,batchedAt):
    clickhouse_host = os.environ.get("CLICKHOUSE_HOST")
    clickhouse_user = os.environ.get("CLICKHOUSE_USER")
    clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD")
    client_1 = await clickhouse_connect.get_async_client(
        host=clickhouse_host,
        user=clickhouse_user,
        password=clickhouse_password,
        secure=True,
    )
    result = await client_1.insert(
        table=table_name,
        data=data,
        column_names=column_names
    )
    logging.info("Completed")
    data_check = await client_1.query(
        f"SELECT MAX(updatedAt),MAX(createdAt),MAX(batchedAt) FROM {table_name} where connection_id = '{connection_id}';"
    )
    await client_1.close()
    logging.info("Data check")
    c_updatedAt = data_check.result_rows[0][0]
    c_createdAt = data_check.result_rows[0][1]
    c_batchedAt = data_check.result_rows[0][2].strftime("%Y-%m-%d %H:%M:%S")
    b_batchedAt = datetime.fromisoformat(batchedAt).strftime("%Y-%m-%d %H:%M:%S")
    logging.info(c_batchedAt,b_batchedAt)
    if c_batchedAt == b_batchedAt:
        logging.info("Data check passed")
        logging.info("Need to add postgres update here")   
        db = SupabaseDatabase()
        query = f'''UPDATE "etl-highlevel-log" 
        SET last_successful_extraction_ts = '{c_updatedAt}',
            is_active = true,  -- Replace with your desired boolean value
            health_status = 'healthy',  -- Replace with your desired status value
            last_error = null,  -- Replace with error details if needed, or null to clear it
            metainfo_updated_ts = '{b_batchedAt}'  -- Uses current timestamp, or specify a value
        WHERE connected_id = '{connection_id}' '''
        result = db.execute_query(query)
        logging.info(result)
    else:
        logging.info("Data check failed")
    return result

if __name__ == "__main__":
    logging.info("Imports Worked??")
    
