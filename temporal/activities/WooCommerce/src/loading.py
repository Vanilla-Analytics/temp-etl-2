from clickhouse_connect import get_async_client
import clickhouse_connect
import asyncio
from datetime import datetime
import os
import logging

async def main(table_name, data, column_names, connection_id, batchedAt):
    try:
        clickhouse_host = os.environ.get("CLICKHOUSE_HOST")
        clickhouse_user = os.environ.get("CLICKHOUSE_USER")
        clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD")
        
        if not all([clickhouse_host, clickhouse_user, clickhouse_password]):
            raise ValueError("Missing ClickHouse connection environment variables")
        
        client = await clickhouse_connect.get_async_client(
            host=clickhouse_host,
            user=clickhouse_user,
            password=clickhouse_password,
            secure=True,
        )
        
        result = await client.insert(
            table=table_name,
            data=data,
            column_names=column_names
        )
        
        logging.info(f"Inserted {len(data)} rows into {table_name}")
        
        # Check the latest data in ClickHouse
        data_check = await client.query(
            f"SELECT MAX(date_modified), MAX(date_created), MAX(batchedAt) FROM `{table_name}` where connected_id = '{connection_id}';"
        )
        
        await client.close()
        
        if data_check.result_rows:
            c_updatedAt = data_check.result_rows[0][0]
            c_createdAt = data_check.result_rows[0][1]
            c_batchedAt = data_check.result_rows[0][2]
            
            if c_batchedAt:
                if isinstance(c_batchedAt, datetime):
                    c_batchedAt_str = c_batchedAt.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    c_batchedAt_str = str(c_batchedAt)
                
                b_batchedAt_dt = datetime.fromisoformat(batchedAt.replace('Z', '+00:00'))
                b_batchedAt_str = b_batchedAt_dt.strftime("%Y-%m-%d %H:%M:%S")
                
                logging.info(f"ClickHouse batchedAt: {c_batchedAt_str}, Current batch: {b_batchedAt_str}")
                
                if c_batchedAt_str == b_batchedAt_str:
                    logging.info("Data check passed")
                    # Update the ETL log in Supabase
                    try:
                        # Import here to avoid circular imports
                        from ....activities.Shopify.src.database import SupabaseDatabase
                        db = SupabaseDatabase()
                        query = f'''UPDATE "etl-highlevel-log" 
                        SET last_successful_extraction_ts = '{c_updatedAt.isoformat() if c_updatedAt else datetime.now().isoformat()}',
                            is_active = true,
                            health_status = 'healthy',
                            last_error = null,
                            metainfo_updated_ts = '{datetime.now().isoformat()}'
                        WHERE connected_id = '{connection_id}' '''
                        result = db.execute_query(query)
                        logging.info(f"Supabase update result: {result}")
                    except Exception as e:
                        logging.error(f"Failed to update Supabase: {e}")
                else:
                    logging.info("Data check failed - batchedAt mismatch")
            else:
                logging.info("No previous batchedAt found in ClickHouse")
        else:
            logging.info("No data returned from ClickHouse check query")
        
        return {"inserted_rows": len(data), "status": "success"}
    
    except Exception as e:
        logging.error(f"Error in loading data to ClickHouse: {e}")
        raise