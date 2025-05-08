import requests
from typing import List,Dict, Any, Optional
from datetime import datetime
from .types import AmazonOrderRequest
import os
import asyncio
class AmazonClient:
    def __init__(self, request: AmazonOrderRequest):
        self.refresh_token = request.refresh_token
        self.region = request.region
        self.CreatedAfter = request.CreatedAfter
        self.LastUpdatedAfter = request.LastUpdatedAfter
        self.MaxResultsPerPage = request.MaxResultsPerPage
        self.client_id = "amzn1.application-oa2-client.144c0aac9aa04fe89ef2efdcc8b16018"
        self.client_secret = request.client_secret
        self.access_token = self._get_access_token()
        self.marketplace_ids = self._get_marketplace_ids(self.region)
        #TODO: Update this to run with doppler for the backend stuff
        self.current_status_code = 0
        self.fill_type = request.fill_type
        self.master_orders = []
    
    def _get_access_token(self) -> str:
        response = requests.post(
            "https://api.amazon.com/auth/o2/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "refresh_token", "refresh_token": self.refresh_token, "client_id": self.client_id, "client_secret": self.client_secret}
        )
        if response.status_code != 200:
            raise Exception(f"Failed to get access token: {response.json()}")   
        return response.json()["access_token"]
    
    async def _enforce_rate_limit(self):
        if self.current_status_code == 429:
            print("Rate limit hit - waiting 60 seconds before retrying...")
            await asyncio.sleep(60)
        elif self.current_status_code == 403:
            print("Access token expired - refreshing token...")
            self.access_token = self._get_access_token()
        elif self.current_status_code >= 500:
            print("Server error - waiting 30 seconds before retrying...")
            await asyncio.sleep(30)
    
    def _interval_constructer(self) -> str:
        # Define timezone offsets for each region
        timezone_offsets = {
            # North America
            'US': '-07:00', 'CA': '-07:00', 'MX': '-06:00', 'BR': '-03:00',
            # Europe
            'ES': '+01:00', 'UK': '+00:00', 'FR': '+01:00', 'BE': '+01:00',
            'NL': '+01:00', 'DE': '+01:00', 'IT': '+01:00', 'SE': '+01:00',
            'ZA': '+02:00', 'PL': '+01:00', 'EG': '+02:00', 'TR': '+03:00',
            'SA': '+03:00', 'AE': '+04:00', 'IN': '+05:30',
            # Far East
            'SG': '+08:00', 'AU': '+10:00', 'JP': '+09:00'
        }
        
        # Get timezone offset for the region
        tz_offset = timezone_offsets.get(self.region)
        if not tz_offset:
            raise ValueError(f"Unsupported region: {self.region}")
        
        # Format start date (CreatedAfter)
        start_date = self.CreatedAfter.strftime("%Y-%m-%d")
        
        # Get today's date for end date
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        # Construct the interval string
        interval = f"{start_date}T00:00:00{tz_offset}--{end_date}T00:00:00{tz_offset}"
        
        return interval

    def _get_base_url_sales(self, region: str) -> str:
        # North America regions
        if region in ['US', 'CA', 'MX', 'BR']:
            return "https://sellingpartnerapi-na.amazon.com/sales/v1/orderMetrics"
        
        # European regions  
        elif region in ['ES', 'UK', 'FR', 'BE', 'NL', 'DE', 'IT', 'SE', 'ZA', 
                       'PL', 'EG', 'TR', 'SA', 'AE', 'IN']:
            return "https://sellingpartnerapi-eu.amazon.com/sales/v1/orderMetrics"
        
        # Far East regions
        elif region in ['SG', 'AU', 'JP']:
            return "https://sellingpartnerapi-fe.amazon.com/sales/v1/orderMetrics"
        
        else:
            raise ValueError(f"Unsupported region: {region}")
    def _get_base_url_orders(self, region: str) -> str:
        # North America regions
        if region in ['US', 'CA', 'MX', 'BR']:
            return "https://sellingpartnerapi-na.amazon.com/orders/v0/orders"
        
        # European regions
        elif region in ['ES', 'UK', 'FR', 'BE', 'NL', 'DE', 'IT', 'SE', 'ZA', 
                       'PL', 'EG', 'TR', 'SA', 'AE', 'IN']:
            return "https://sellingpartnerapi-eu.amazon.com/orders/v0/orders"
        
        # Far East regions
        elif region in ['SG', 'AU', 'JP']:
            return "https://sellingpartnerapi-fe.amazon.com/orders/v0/orders"
        
        else:
            raise ValueError(f"Unsupported region: {region}")
    def _get_marketplace_ids(self,region:str) -> str:
        marketplace_mapping = {
        # North America
        'CA': 'A2EUQ1WTGCTBG2',
        'US': 'ATVPDKIKX0DER',
        'MX': 'A1AM78C64UM0Y8',
        'BR': 'A2Q3Y263D00KWC',
        
        # Europe
        'IE': 'A28R8C7NBKEWEA', 
        'ES': 'A1RKKUPIHCS9HS',
        'UK': 'A1F83G8C2ARO7P',
        'FR': 'A13V1IB3VIYZZH',
        'BE': 'AMEN7PMS3EDWL',
        'NL': 'A1805IZSGTT6HS',
        'DE': 'A1PA6795UKMFR9',
        'IT': 'APJ6JRA9NG5V4',
        'SE': 'A2NODRKZP88ZB9',
        'ZA': 'AE08WJ6YKNBMC',
        'PL': 'A1C3SOZRARQ6R3',
        'EG': 'ARBP9OOSHTCHU',
        'TR': 'A33AVAJ2PDY3EV',
        'SA': 'A17E79C6D8DWNP',
        'AE': 'A2VIGQ35RCS4UG',
        'IN': 'A21TJRUUN4KGV',
        
        # Far East
        'SG': 'A19VAU5U5O7RUS',
        'AU': 'A39IBJ37TRP1C6',
        'JP': 'A1VC38T7YXB528'
        }
        return marketplace_mapping.get(region,None)
    
    def _get_headers(self) -> Dict[str,str]:
        return {
            "x-amz-access-token": self.access_token
        }
    
    async def get_sales(self):
        max_retries = 3
        
        retry_count = 0
        while retry_count < max_retries:
            try:
                await self._enforce_rate_limit()
                base_url = self._get_base_url_sales(self.region)
                headers = self._get_headers()
                
                params = {
                    "marketplaceIds": self.marketplace_ids,
                    "interval": self._interval_constructer(),
                    'granularity': 'Day'
                }
                
                response = requests.get(base_url, headers=headers, params=params)
                self.current_status_code = response.status_code
                
                if response.status_code == 200:
                    # Return the JSON data instead of the response object
                    return response.json()
                
                elif response.status_code in [429, 403, 500, 502, 503, 504]:
                    print(f"Received status code {response.status_code}, attempt {retry_count + 1} of {max_retries}")
                    retry_count += 1
                    if retry_count == max_retries:
                        raise Exception(f"Max retries reached. Last status code: {response.status_code}")
                    continue
                
                else:
                    raise Exception(f"Unexpected status code: {response.status_code}")

            except Exception as e:
                print(f"Error fetching orders: {e}")
                retry_count += 1
                if retry_count == max_retries:
                    raise
                await asyncio.sleep(5 * retry_count)  # Progressive backoff

    async def get_orders(self, nextToken: Optional[str] = None):
        master_orders = []
        current_token = nextToken
        max_retries = 3
        
        while True:
            retry_count = 0
            while retry_count < max_retries:
                try:
                    await self._enforce_rate_limit()
                    base_url = self._get_base_url_orders(self.region)
                    headers = self._get_headers()
                    
                    params = {
                        "MarketplaceIds": self.marketplace_ids,
                        "MaxResultsPerPage": self.MaxResultsPerPage
                    }
                    
                    if current_token is not None:
                        params["NextToken"] = current_token
                    elif self.fill_type == "backfill":
                        params["CreatedAfter"] = self.CreatedAfter.strftime("%Y-%m-%d")
                    elif self.fill_type == "incremental":
                        params["LastUpdatedAfter"] = self.LastUpdatedAfter.strftime("%Y-%m-%d")

                    response = requests.get(base_url, headers=headers, params=params)
                    self.current_status_code = response.status_code
                    
                    if response.status_code == 200:
                        result = response.json()
                        orders_data = result.get("payload", {}).get("Orders", [])
                        master_orders.extend(orders_data)
                        
                        current_token = result.get("payload", {}).get("NextToken")
                        if current_token:
                            print(f"Fetched {len(orders_data)} orders. Getting next page...")
                        else:
                            print("No more pages to fetch")
                            break
                        break  # Success, exit retry loop
                    
                    elif response.status_code in [429, 403, 500, 502, 503, 504]:
                        print(f"Received status code {response.status_code}, attempt {retry_count + 1} of {max_retries}")
                        retry_count += 1
                        if retry_count == max_retries:
                            raise Exception(f"Max retries reached. Last status code: {response.status_code}")
                        continue
                    
                    else:
                        raise Exception(f"Unexpected status code: {response.status_code}")

                except Exception as e:
                    print(f"Error fetching orders: {e}")
                    retry_count += 1
                    if retry_count == max_retries:
                        raise
                    await asyncio.sleep(5 * retry_count)  # Progressive backoff
            
            if not current_token:
                break

        print(f"Total orders fetched: {len(master_orders)}")
        return master_orders



