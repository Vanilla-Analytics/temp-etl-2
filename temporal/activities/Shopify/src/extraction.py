import requests
from typing import Optional, Dict, Any, List, Generator
from datetime import datetime
import re
import json
import asyncio
import time

class ShopifyClient:

    def __init__(
        self, 
        shop_name: str, 
        access_token: str,
        fill_type: str,
        start_time: datetime,
        end_time: Optional[datetime] = None
    ):
        self.api_version = "2025-01"
        self.base_url = f"https://{shop_name}/admin/api/{self.api_version}/graphql.json"
        self.access_token = access_token
        self._last_request_times = []
        if fill_type == "backfill":
            self.filter_query = self._build_date_query_backfill(start_time, end_time)
        elif fill_type == "incremental":
            self.filter_query = self._build_date_query_new_fetch(start_time, end_time)
        else:
            raise ValueError(f"Invalid fill type: {fill_type}")


    def _get_headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.access_token
        }
    # TODO: Change to async io to make data pull cleaner and avoid thread blocking
    async def _enforce_rate_limit(self, current_credits: int):
        """Implement rate limiting based on Shopify's throttling information"""
        try:
            if current_credits < 250:
                #print(f"Available points low ({current_credits}), waiting 5 seconds")
                await asyncio.sleep(5)
        # TODO: Fix this because its not the right thing to have here        
        except (KeyError, TypeError) as e:
            #print(f"Could not parse throttle status from response: {e}")
            # If we can't get the throttle status, wait 1 second to be safe
            await asyncio.sleep(1)

    def _format_datetime(self, dt: datetime, is_end_time: bool = False) -> str:
        """
        Format datetime to Shopify's expected format.
        If only date is provided (time is midnight), format accordingly.
        """
        if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0:
            # Only date was provided
            if is_end_time:
                # Set to end of day: 23:59:59.999Z
                dt = dt.replace(hour=23, minute=59, second=59)
            # else keep start of day: 00:00:00.000Z
        
        # Convert to UTC format with 'Z' suffix
        #print(dt.astimezone().strftime('%Y-%m-%dT%H:%M:%S')[:-3] + 'Z')
        return dt.astimezone().strftime('%Y-%m-%dT%H:%M:%S')[:-3] + 'Z'

    def _build_date_query_backfill(
        self, 
        start_time: datetime, 
        end_time: Optional[datetime] = None
    ) -> str:
        query_parts = f"created_at:>=\'{self._format_datetime(start_time)}\'"
        if end_time:
            query_parts = "(" + query_parts
            query_parts += f" AND created_at:<=\'{self._format_datetime(end_time, is_end_time=True)}\')"
        return query_parts
    
    def _build_date_query_new_fetch(
        self, 
        start_time: datetime, 
        end_time: Optional[datetime] = None
    ) -> str:
        formatted_start = self._format_datetime(start_time)
        query_parts = f"(created_at:>=\'{formatted_start}\' OR updated_at:>=\'{formatted_start}\')"
        if end_time:
            formatted_end = self._format_datetime(end_time, is_end_time=True)
            query_parts = f"({query_parts} AND (created_at:<=\'{formatted_end}\' OR updated_at:<=\'{formatted_end}\'))"
        return query_parts
    def _return_data_points(self) -> str:
        return """
                nodes {
                id
                name
                createdAt
                updatedAt
                cancelledAt
                cancelReason
                currencyCode
                totalDiscountsSet {
                    shopMoney {
                    amount
                    }
                }
                totalPriceSet {
                    shopMoney {
                    amount
                    }
                }
                totalReceivedSet {
                    shopMoney {
                    amount
                    }
                }
                totalRefundedSet {
                    shopMoney {
                    amount
                    }
                }
                totalShippingPriceSet {
                    shopMoney {
                    amount
                    }
                }
                totalTaxSet {
                    shopMoney {
                    amount
                    }
                }
                totalTipReceivedSet {
                    shopMoney {
                    amount
                    }
                }
                currentCartDiscountAmountSet {
                    shopMoney {
                    amount
                    }
                }
                currentShippingPriceSet {
                    shopMoney {
                    amount
                    }
                }
                currentSubtotalLineItemsQuantity
                currentSubtotalPriceSet {
                    shopMoney {
                    amount
                    }
                }
                currentTotalAdditionalFeesSet {
                    shopMoney {
                    amount
                    }
                }
                currentTotalDiscountsSet {
                    shopMoney {
                    amount
                    }
                }
                currentTotalDutiesSet {
                    shopMoney {
                    amount
                    }
                }
                currentTotalTaxSet {
                    shopMoney {
                    amount
                    }
                }
                currentTotalPriceSet {
                    shopMoney {
                    amount
                    }
                }
                netPaymentSet {
                    shopMoney {
                    amount
                    }
                }
                discountCode
                displayFinancialStatus
                displayFulfillmentStatus
                dutiesIncluded
                fullyPaid
                lineItems(first: 10) {
                    edges {
                    node {
                        id
                        name
                        sku
                        title
                        quantity
                        currentQuantity
                        originalTotalSet {
                        shopMoney {
                            amount
                        }
                        }
                        variantTitle
                        originalUnitPriceSet {
                        shopMoney {
                            amount
                        }
                        }
                        totalDiscountSet {
                        shopMoney {
                            amount
                        }
                        }
                        discountedUnitPriceAfterAllDiscountsSet {
                        shopMoney {
                            amount
                        }
                        }
                    }
                    }
                }
                note
                paymentGatewayNames
                processedAt
                tags
                taxesIncluded
                taxExempt
                unpaid
                test
                }
                pageInfo {
                hasPreviousPage
                hasNextPage
                startCursor
                endCursor
                }
        """
    async def get_orders(
        self,
        cursor: Optional[str] = None,
        credits: Optional[int] = None
    ):
        """
        Fetch all orders within the given date range, handling pagination automatically.
        
        Args:
            cursor: Pagination cursor for fetching next page
            credits: Available API credits for rate limiting
            
        Returns:
            List of all orders within the specified date range
        """
        first = 250
        mid_payload = self._return_data_points()
        end_payload = "}}"
        if cursor is not None:
            query_text = f"""query {{ orders(after: "{cursor}", first: {first}, query: "{self.filter_query}") {{ nodes {{ id name createdAt updatedAt cancelledAt cancelReason currencyCode currentCartDiscountAmountSet {{shopMoney {{amount currencyCode}}}} currentShippingPriceSet {{shopMoney {{amount currencyCode}}}} currentSubtotalLineItemsQuantity currentSubtotalPriceSet {{shopMoney {{amount currencyCode}}}} currentTotalAdditionalFeesSet {{shopMoney {{amount currencyCode}}}} currentTotalDiscountsSet {{shopMoney {{amount currencyCode}}}} currentTotalDutiesSet {{shopMoney {{amount currencyCode}}}} currentTotalTaxSet {{shopMoney {{amount currencyCode}}}} currentTotalPriceSet {{shopMoney {{amount currencyCode}}}} netPaymentSet {{shopMoney {{amount currencyCode}}}} discountCode displayFinancialStatus displayFulfillmentStatus dutiesIncluded fullyPaid lineItems(first: 10) {{edges {{node {{id name sku title quantity discountedUnitPriceSet{{shopMoney{{amount}}}} }}}}}} note paymentGatewayNames processedAt tags taxesIncluded taxExempt unpaid test }} pageInfo {{ hasPreviousPage hasNextPage startCursor endCursor }} }} }}"""
            start_payload = f"""query {{ orders(after: "{cursor}", first: {first}, query: "{self.filter_query}") {{"""
        else:
            query_text = f"""query {{ orders(first: {first}, query: "{self.filter_query}") {{ nodes {{ id name createdAt updatedAt cancelledAt cancelReason currencyCode currentCartDiscountAmountSet {{shopMoney {{amount currencyCode}}}} currentShippingPriceSet {{shopMoney {{amount currencyCode}}}} currentSubtotalLineItemsQuantity currentSubtotalPriceSet {{shopMoney {{amount currencyCode}}}} currentTotalAdditionalFeesSet {{shopMoney {{amount currencyCode}}}} currentTotalDiscountsSet {{shopMoney {{amount currencyCode}}}} currentTotalDutiesSet {{shopMoney {{amount currencyCode}}}} currentTotalTaxSet {{shopMoney {{amount currencyCode}}}} currentTotalPriceSet {{shopMoney {{amount currencyCode}}}} netPaymentSet {{shopMoney {{amount currencyCode}}}} discountCode displayFinancialStatus displayFulfillmentStatus dutiesIncluded fullyPaid lineItems(first: 10) {{edges {{node {{id name sku title quantity discountedUnitPriceSet{{shopMoney{{amount}}}} }}}}}} note paymentGatewayNames processedAt tags taxesIncluded taxExempt unpaid test }} pageInfo {{ hasPreviousPage hasNextPage startCursor endCursor }} }} }}"""
            start_payload = f"""query {{ orders(first: {first}, query: "{self.filter_query}") {{"""
        final_payload = start_payload + mid_payload + end_payload
        try:
            if credits is not None:
                await self._enforce_rate_limit(credits)
            
            #print(f"Fetching orders{' after ' + cursor if cursor else ''}")
            base_url = self.base_url
            headers = self._get_headers()
            payload = {
                "query": final_payload
            }
            response = requests.post(base_url, headers=headers, json=payload)
            #print(response.json())
            response.raise_for_status()
            result = response.json()                   
            orders_data = result.get("data", {}).get("orders", {}).get("nodes", [])
            page_info = result.get("data", {}).get("orders", {}).get("pageInfo", {})
            master_orders = []

            # Add current page of orders
            master_orders.extend(orders_data)

            # If there are more pages, fetch them recursively
            if page_info.get("hasNextPage"):
                cursor = page_info["endCursor"]
                credits = result.get("extensions", {}).get("cost", {}).get("throttleStatus", {}).get("currentlyAvailable", 0)
                next_page_orders = await self.get_orders(cursor=cursor, credits=credits)
                master_orders.extend(next_page_orders)
            
            return master_orders
        # TODO: Add error handling
        except Exception as e:
            print(f"Error fetching orders: {str(e)}")
            raise

