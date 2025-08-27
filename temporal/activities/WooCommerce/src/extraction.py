import requests
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from urllib.parse import urljoin
import asyncio
import logging
from .types import WooOrderRequest

class WooClient:
    def __init__(self, request: WooOrderRequest):
        self.base_url = request.base_url.rstrip('/')
        self.consumer_key = request.consumer_key
        self.consumer_secret = request.consumer_secret
        self.connected_id = request.connected_id
        self.fill_type = request.fill_type
        self.CreatedAfter = request.CreatedAfter
        self.LastUpdatedAfter = request.LastUpdatedAfter
        self.PerPage = request.PerPage or 100
        self.current_status_code = 0
        
    def _get_headers(self) -> Dict[str, str]:
        return {"Accept": "application/json"}
    
    def _get_auth(self) -> tuple:
        return (self.consumer_key, self.consumer_secret)
    
    def _build_params(self, page: int = 1) -> Dict[str, Any]:
        params = {
            "page": page,
            "per_page": self.PerPage,
            "orderby": "date",
            "order": "asc"
        }
        
        if self.fill_type == "backfill" and self.CreatedAfter:
            params["after"] = self.CreatedAfter.astimezone(timezone.utc).isoformat()
        elif self.fill_type == "incremental" and self.LastUpdatedAfter:
            params["modified_after"] = self.LastUpdatedAfter.astimezone(timezone.utc).isoformat()
        
        return params
    
    async def _enforce_rate_limit(self):
        if self.current_status_code == 429:
            logging.warning("Rate limit hit - waiting 60 seconds before retrying...")
            await asyncio.sleep(60)
        elif self.current_status_code >= 500:
            logging.warning("Server error - waiting 30 seconds before retrying...")
            await asyncio.sleep(30)
    
    async def get_orders(self) -> List[Dict[str, Any]]:
        master_orders = []
        page = 1
        max_retries = 3
        
        while True:
            retry_count = 0
            while retry_count < max_retries:
                try:
                    await self._enforce_rate_limit()
                    
                    endpoint = urljoin(self.base_url + '/', 'wp-json/wc/v3/orders')
                    params = self._build_params(page)
                    
                    response = requests.get(
                        endpoint,
                        auth=self._get_auth(),
                        headers=self._get_headers(),
                        params=params,
                        timeout=30
                    )
                    
                    self.current_status_code = response.status_code
                    
                    if response.status_code == 200:
                        orders_data = response.json()
                        master_orders.extend(orders_data)
                        
                        # Check pagination
                        total_pages = int(response.headers.get('X-WP-TotalPages', 1))
                        if page >= total_pages or len(orders_data) < self.PerPage:
                            logging.info(f"Total orders fetched: {len(master_orders)}")
                            return master_orders
                        
                        page += 1
                        logging.info(f"Fetched {len(orders_data)} orders. Getting next page...")
                        break
                    
                    elif response.status_code in [429, 403, 500, 502, 503, 504]:
                        logging.warning(f"Received status code {response.status_code}, attempt {retry_count + 1} of {max_retries}")
                        retry_count += 1
                        if retry_count == max_retries:
                            raise Exception(f"Max retries reached. Last status code: {response.status_code}")
                        await asyncio.sleep(5 * retry_count)
                        continue
                    
                    else:
                        raise Exception(f"Unexpected status code: {response.status_code}, Response: {response.text}")
                        
                except Exception as e:
                    logging.error(f"Error fetching orders: {e}")
                    retry_count += 1
                    if retry_count == max_retries:
                        raise
                    await asyncio.sleep(5 * retry_count)
            
            if page > 100:  # Safety break
                break
        
        return master_orders