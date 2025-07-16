import asyncio
import aiohttp
import json
import time
import hmac
import hashlib
import base64
from typing import Optional, Dict, List, Any
from datetime import datetime
from config import config
from logging_setup import get_logger
from db.redis_client import get_database

logger = get_logger("paypal")

class PayPalClient:
    """PayPal API client for payment processing and webhook handling."""
    
    def __init__(self):
        # Use sandbox or live environment based on config
        if config.paypal_environment == "sandbox":
            self.base_url = "https://api-m.sandbox.paypal.com"
        else:
            self.base_url = "https://api-m.paypal.com"
        
        self.client_id = config.paypal_client_id
        self.client_secret = config.paypal_client_secret
        self.session: Optional[aiohttp.ClientSession] = None
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[float] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session
    
    async def close(self):
        """Close the aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def _get_access_token(self) -> Optional[str]:
        """Get or refresh PayPal access token."""
        # Check if current token is still valid
        if self.access_token and self.token_expires_at:
            if time.time() < self.token_expires_at - 60:  # Refresh 1 minute before expiry
                return self.access_token
        
        try:
            url = f"{self.base_url}/v1/oauth2/token"
            
            # Prepare authentication
            auth_string = f"{self.client_id}:{self.client_secret}"
            auth_bytes = auth_string.encode('ascii')
            auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
            
            headers = {
                "Accept": "application/json",
                "Accept-Language": "en_US",
                "Authorization": f"Basic {auth_b64}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            
            data = "grant_type=client_credentials"
            
            session = await self._get_session()
            
            async with session.post(url, data=data, headers=headers) as response:
                if response.status == 200:
                    token_data = await response.json()
                    self.access_token = token_data.get("access_token")
                    expires_in = token_data.get("expires_in", 3600)
                    self.token_expires_at = time.time() + expires_in
                    
                    logger.info("PayPal access token obtained successfully")
                    return self.access_token
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get PayPal access token ({response.status}): {error_text}")
                    return None
        
        except Exception as e:
            logger.exception(f"Exception getting PayPal access token: {e}")
            return None
    
    async def _get_headers(self) -> Optional[Dict[str, str]]:
        """Get headers with valid access token."""
        token = await self._get_access_token()
        if not token:
            return None
        
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "PayPal-Request-Id": str(int(time.time() * 1000))  # Unique request ID
        }
    
    async def _log_payment_event(self, event_type: str, data: Dict[str, Any]):
        """Log payment event to database for analytics."""
        try:
            db = await get_database()
            event_data = {
                "type": event_type,
                "platform": "paypal",
                "timestamp": datetime.utcnow().isoformat(),
                "data": json.dumps(data)
            }
            
            event_key = f"payment_event:paypal:{int(time.time())}"
            await db.hset(event_key, mapping=event_data)
            
            # Add to payment events list for analytics
            await db.lpush("payment_events:paypal", event_key)
            
            # Log sales events separately
            if event_type in ["payment_completed", "payment_created"]:
                logger.info(f"SALES EVENT: {event_type} - {data.get('amount', 'N/A')}")
            
        except Exception as e:
            logger.error(f"Error logging PayPal payment event: {e}")
    
    async def create_order(self, amount: str, currency: str = "USD", description: str = "", return_url: str = "", cancel_url: str = "") -> Optional[Dict[str, Any]]:
        """Create a PayPal order."""
        headers = await self._get_headers()
        if not headers:
            return None
        
        try:
            url = f"{self.base_url}/v2/checkout/orders"
            
            order_data = {
                "intent": "CAPTURE",
                "purchase_units": [{
                    "amount": {
                        "currency_code": currency,
                        "value": amount
                    },
                    "description": description
                }],
                "application_context": {
                    "return_url": return_url or "https://example.com/return",
                    "cancel_url": cancel_url or "https://example.com/cancel"
                }
            }
            
            session = await self._get_session()
            
            async with session.post(url, json=order_data, headers=headers) as response:
                if response.status == 201:
                    order = await response.json()
                    
                    await self._log_payment_event("order_created", {
                        "order_id": order.get("id"),
                        "amount": amount,
                        "currency": currency,
                        "description": description
                    })
                    
                    logger.info(f"SALES: PayPal order created successfully: {order.get('id')}")
                    return order
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to create PayPal order ({response.status}): {error_text}")
                    return None
        
        except Exception as e:
            logger.exception(f"Exception creating PayPal order: {e}")
            return None
    
    async def capture_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Capture a PayPal order."""
        headers = await self._get_headers()
        if not headers:
            return None
        
        try:
            url = f"{self.base_url}/v2/checkout/orders/{order_id}/capture"
            session = await self._get_session()
            
            async with session.post(url, headers=headers) as response:
                if response.status == 201:
                    capture_data = await response.json()
                    
                    # Extract payment details
                    purchase_units = capture_data.get("purchase_units", [])
                    if purchase_units:
                        payments = purchase_units[0].get("payments", {})
                        captures = payments.get("captures", [])
                        if captures:
                            capture = captures[0]
                            amount = capture.get("amount", {})
                            
                            await self._log_payment_event("payment_completed", {
                                "order_id": order_id,
                                "capture_id": capture.get("id"),
                                "amount": amount.get("value"),
                                "currency": amount.get("currency_code"),
                                "status": capture.get("status")
                            })
                    
                    logger.info(f"PAYMENT: PayPal order captured successfully: {order_id}")
                    return capture_data
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to capture PayPal order ({response.status}): {error_text}")
                    return None
        
        except Exception as e:
            logger.exception(f"Exception capturing PayPal order: {e}")
            return None
    
    async def get_order_details(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Get details of a PayPal order."""
        headers = await self._get_headers()
        if not headers:
            return None
        
        try:
            url = f"{self.base_url}/v2/checkout/orders/{order_id}"
            session = await self._get_session()
            
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    order_details = await response.json()
                    logger.info(f"Retrieved PayPal order details: {order_id}")
                    return order_details
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get PayPal order details ({response.status}): {error_text}")
                    return None
        
        except Exception as e:
            logger.exception(f"Exception getting PayPal order details: {e}")
            return None
    
    async def refund_capture(self, capture_id: str, amount: Optional[str] = None, currency: str = "USD", note: str = "") -> Optional[Dict[str, Any]]:
        """Refund a captured payment."""
        headers = await self._get_headers()
        if not headers:
            return None
        
        try:
            url = f"{self.base_url}/v2/payments/captures/{capture_id}/refund"
            
            refund_data = {
                "note_to_payer": note
            }
            
            if amount:
                refund_data["amount"] = {
                    "value": amount,
                    "currency_code": currency
                }
            
            session = await self._get_session()
            
            async with session.post(url, json=refund_data, headers=headers) as response:
                if response.status == 201:
                    refund = await response.json()
                    
                    await self._log_payment_event("payment_refunded", {
                        "capture_id": capture_id,
                        "refund_id": refund.get("id"),
                        "amount": amount,
                        "currency": currency,
                        "note": note
                    })
                    
                    logger.info(f"PAYMENT: PayPal refund processed successfully: {refund.get('id')}")
                    return refund
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to process PayPal refund ({response.status}): {error_text}")
                    return None
        
        except Exception as e:
            logger.exception(f"Exception processing PayPal refund: {e}")
            return None
    
    async def create_subscription(self, plan_id: str, subscriber_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a PayPal subscription."""
        headers = await self._get_headers()
        if not headers:
            return None
        
        try:
            url = f"{self.base_url}/v1/billing/subscriptions"
            
            subscription_data = {
                "plan_id": plan_id,
                "subscriber": subscriber_info,
                "application_context": {
                    "brand_name": "AURELIUS",
                    "locale": "en-US",
                    "shipping_preference": "NO_SHIPPING",
                    "user_action": "SUBSCRIBE_NOW",
                    "payment_method": {
                        "payer_selected": "PAYPAL",
                        "payee_preferred": "IMMEDIATE_PAYMENT_REQUIRED"
                    },
                    "return_url": "https://example.com/return",
                    "cancel_url": "https://example.com/cancel"
                }
            }
            
            session = await self._get_session()
            
            async with session.post(url, json=subscription_data, headers=headers) as response:
                if response.status == 201:
                    subscription = await response.json()
                    
                    await self._log_payment_event("subscription_created", {
                        "subscription_id": subscription.get("id"),
                        "plan_id": plan_id,
                        "status": subscription.get("status")
                    })
                    
                    logger.info(f"SALES: PayPal subscription created successfully: {subscription.get('id')}")
                    return subscription
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to create PayPal subscription ({response.status}): {error_text}")
                    return None
        
        except Exception as e:
            logger.exception(f"Exception creating PayPal subscription: {e}")
            return None
    
    async def verify_webhook_signature(self, headers: Dict[str, str], body: str, webhook_id: str) -> bool:
        """Verify PayPal webhook signature."""
        try:
            # Get webhook verification data
            auth_algo = headers.get("PAYPAL-AUTH-ALGO")
            transmission_id = headers.get("PAYPAL-TRANSMISSION-ID")
            cert_id = headers.get("PAYPAL-CERT-ID")
            transmission_sig = headers.get("PAYPAL-TRANSMISSION-SIG")
            transmission_time = headers.get("PAYPAL-TRANSMISSION-TIME")
            
            if not all([auth_algo, transmission_id, cert_id, transmission_sig, transmission_time]):
                logger.error("Missing required webhook signature headers")
                return False
            
            # Verify with PayPal API
            verify_headers = await self._get_headers()
            if not verify_headers:
                return False
            
            url = f"{self.base_url}/v1/notifications/verify-webhook-signature"
            
            verify_data = {
                "auth_algo": auth_algo,
                "cert_id": cert_id,
                "transmission_id": transmission_id,
                "transmission_sig": transmission_sig,
                "transmission_time": transmission_time,
                "webhook_id": webhook_id,
                "webhook_event": json.loads(body)
            }
            
            session = await self._get_session()
            
            async with session.post(url, json=verify_data, headers=verify_headers) as response:
                if response.status == 200:
                    verification = await response.json()
                    verification_status = verification.get("verification_status")
                    
                    if verification_status == "SUCCESS":
                        logger.info("PayPal webhook signature verified successfully")
                        return True
                    else:
                        logger.warning(f"PayPal webhook signature verification failed: {verification_status}")
                        return False
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to verify PayPal webhook signature ({response.status}): {error_text}")
                    return False
        
        except Exception as e:
            logger.exception(f"Exception verifying PayPal webhook signature: {e}")
            return False
    
    async def handle_webhook_event(self, event_data: Dict[str, Any]) -> bool:
        """Handle PayPal webhook events."""
        try:
            event_type = event_data.get("event_type")
            resource = event_data.get("resource", {})
            
            logger.info(f"Processing PayPal webhook event: {event_type}")
            
            # Handle different event types
            if event_type == "PAYMENT.CAPTURE.COMPLETED":
                await self._handle_payment_completed(resource)
            elif event_type == "PAYMENT.CAPTURE.DENIED":
                await self._handle_payment_denied(resource)
            elif event_type == "PAYMENT.CAPTURE.REFUNDED":
                await self._handle_payment_refunded(resource)
            elif event_type == "BILLING.SUBSCRIPTION.CREATED":
                await self._handle_subscription_created(resource)
            elif event_type == "BILLING.SUBSCRIPTION.CANCELLED":
                await self._handle_subscription_cancelled(resource)
            else:
                logger.info(f"Unhandled PayPal webhook event type: {event_type}")
            
            # Log the webhook event
            await self._log_payment_event(f"webhook_{event_type.lower()}", {
                "event_type": event_type,
                "resource_id": resource.get("id"),
                "resource_type": resource.get("resource_type")
            })
            
            return True
        
        except Exception as e:
            logger.exception(f"Exception handling PayPal webhook event: {e}")
            return False
    
    async def _handle_payment_completed(self, resource: Dict[str, Any]):
        """Handle completed payment."""
        try:
            db = await get_database()
            
            payment_data = {
                "capture_id": resource.get("id"),
                "amount": resource.get("amount", {}).get("value"),
                "currency": resource.get("amount", {}).get("currency_code"),
                "status": "completed",
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Store payment record
            payment_key = f"payment:completed:{resource.get('id')}"
            await db.hset(payment_key, mapping=payment_data)
            
            # Update sales metrics
            await db.incr("sales:total_count")
            if payment_data["amount"]:
                current_total = await db.get("sales:total_amount") or "0"
                new_total = float(current_total) + float(payment_data["amount"])
                await db.set("sales:total_amount", str(new_total))
            
            logger.info(f"SALES: Payment completed - {payment_data['amount']} {payment_data['currency']}")
            
        except Exception as e:
            logger.exception(f"Error handling completed payment: {e}")
    
    async def _handle_payment_denied(self, resource: Dict[str, Any]):
        """Handle denied payment."""
        logger.warning(f"PAYMENT: Payment denied - {resource.get('id')}")
        
        # Could trigger follow-up actions here
        # e.g., send notification, update customer record, etc.
    
    async def _handle_payment_refunded(self, resource: Dict[str, Any]):
        """Handle refunded payment."""
        try:
            db = await get_database()
            
            # Update refund metrics
            await db.incr("sales:refund_count")
            refund_amount = resource.get("amount", {}).get("value")
            if refund_amount:
                current_total = await db.get("sales:refund_amount") or "0"
                new_total = float(current_total) + float(refund_amount)
                await db.set("sales:refund_amount", str(new_total))
            
            logger.info(f"PAYMENT: Refund processed - {refund_amount}")
            
        except Exception as e:
            logger.exception(f"Error handling refunded payment: {e}")
    
    async def _handle_subscription_created(self, resource: Dict[str, Any]):
        """Handle subscription created."""
        logger.info(f"SALES: Subscription created - {resource.get('id')}")
    
    async def _handle_subscription_cancelled(self, resource: Dict[str, Any]):
        """Handle subscription cancelled."""
        logger.info(f"SALES: Subscription cancelled - {resource.get('id')}")
    
    async def get_payment_analytics(self) -> Dict[str, Any]:
        """Get payment analytics data."""
        try:
            db = await get_database()
            
            analytics = {
                "total_sales_count": int(await db.get("sales:total_count") or 0),
                "total_sales_amount": float(await db.get("sales:total_amount") or 0),
                "total_refund_count": int(await db.get("sales:refund_count") or 0),
                "total_refund_amount": float(await db.get("sales:refund_amount") or 0),
                "net_sales_amount": 0
            }
            
            analytics["net_sales_amount"] = analytics["total_sales_amount"] - analytics["total_refund_amount"]
            
            return analytics
        
        except Exception as e:
            logger.exception(f"Error getting payment analytics: {e}")
            return {}

# Global PayPal client instance
_paypal_client: Optional[PayPalClient] = None

async def get_paypal_client() -> PayPalClient:
    """Get PayPal client instance."""
    global _paypal_client
    if _paypal_client is None:
        _paypal_client = PayPalClient()
    return _paypal_client

async def close_paypal_client():
    """Close PayPal client."""
    global _paypal_client
    if _paypal_client:
        await _paypal_client.close()
        _paypal_client = None

# Convenience functions
async def create_order(amount: str, currency: str = "USD", description: str = "", return_url: str = "", cancel_url: str = "") -> Optional[Dict[str, Any]]:
    """Create a PayPal order using the global client."""
    client = await get_paypal_client()
    return await client.create_order(amount, currency, description, return_url, cancel_url)

async def capture_order(order_id: str) -> Optional[Dict[str, Any]]:
    """Capture a PayPal order using the global client."""
    client = await get_paypal_client()
    return await client.capture_order(order_id)

async def get_order_details(order_id: str) -> Optional[Dict[str, Any]]:
    """Get order details using the global client."""
    client = await get_paypal_client()
    return await client.get_order_details(order_id)

async def refund_capture(capture_id: str, amount: Optional[str] = None, currency: str = "USD", note: str = "") -> Optional[Dict[str, Any]]:
    """Refund a capture using the global client."""
    client = await get_paypal_client()
    return await client.refund_capture(capture_id, amount, currency, note)

async def handle_webhook_event(event_data: Dict[str, Any]) -> bool:
    """Handle webhook event using the global client."""
    client = await get_paypal_client()
    return await client.handle_webhook_event(event_data)

async def verify_webhook_signature(headers: Dict[str, str], body: str, webhook_id: str) -> bool:
    """Verify webhook signature using the global client."""
    client = await get_paypal_client()
    return await client.verify_webhook_signature(headers, body, webhook_id)

async def get_payment_analytics() -> Dict[str, Any]:
    """Get payment analytics using the global client."""
    client = await get_paypal_client()
    return await client.get_payment_analytics()
