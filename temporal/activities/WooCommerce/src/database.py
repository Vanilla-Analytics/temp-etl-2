import json
from typing import Optional
import pandas as pd
import psycopg2
import os
import logging
import sentry_sdk

class SupabaseDatabase:
    def __init__(self) -> None:
        self.host = os.environ.get("SUPABASE_HOST")
        self.dbname = os.environ.get("SUPABASE_DBNAME")
        self.user = os.environ.get("SUPABASE_USER")
        self.password = os.environ.get("SUPABASE_PASSWORD")
        self.port = os.environ.get("SUPABASE_PORT")

        if not all([self.host, self.dbname, self.user, self.password, self.port]):
            raise RuntimeError(
                "Missing environment variables for Supabase database connection"
            )

    def get_data(self, query, params=None) -> Optional[pd.DataFrame]:
        try:
            with psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(cursor.fetchall(), columns=columns)
            return df if not df.empty else None
        except Exception as e:
            logging.error(f"Database query error: {e}")
            return None

    def execute_query(self, query, params=None) -> bool:
        try:
            with psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    rows_affected = cursor.rowcount
                    conn.commit()

            logging.info(f"Query executed successfully. Rows affected: {rows_affected}")
            return rows_affected > 0
        except psycopg2.Error as e:
            logging.error(f"Database error: {str(e)}")
            logging.error(f"Database error: {e.pgcode} - {e.pgerror}")
            sentry_sdk.capture_exception(e)
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
        return False