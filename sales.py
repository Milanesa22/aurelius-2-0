import asyncio
import json
import time
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
from config import config
from logging_setup import get_logger
from db.redis_client import get_database
from modules import core_ai
from modules.social import twitter, mastodon, discord
from modules.payment import paypal

logger = get_logger("sales")

class SalesHandler:
    """Sales coordination and management system."""
    
    def __init__(self):
        self.db = None
        self.sales_prompts = {
            "initial_contact": """You are a professional sales assistant for AURELIUS, an autonomous business management system. 
            Create a personalized, helpful response that:
            - Addresses the customer's specific inquiry
            - Highlights relevant benefits of our solution
            - Uses a consultative, not pushy approach
            - Includes a soft call-to-action
            - Maintains professionalism while being friendly
            
            Customer inquiry: {inquiry}
            Customer context: {context}""",
            
            "follow_up": """You are following up on a previous sales conversation for AURELIUS.
            Create a helpful follow-up message that:
            - References the previous conversation naturally
            - Provides additional value or information
            - Addresses any concerns that were raised
            - Moves the conversation forward gently
            - Maintains the relationship focus
            
            Previous conversation: {previous_conversation}
            Customer response: {customer_response}
            Days since last contact: {days_since_contact}""",
            
            "objection_handling": """You are handling a customer objection for AURELIUS sales.
            Create a response that:
            - Acknowledges the customer's concern respectfully
            - Provides a thoughtful, evidence-based response
            - Offers alternatives or solutions
            - Maintains trust and credibility
            - Keeps the conversation constructive
            
            Customer objection: {objection}
            Context: {context}""",
            
            "closing": """You are helping close a sale for AURELIUS.
            Create a closing message that:
            - Summarizes the key benefits discussed
            - Creates appropriate urgency without pressure
            - Makes the next steps clear and easy
            - Addresses final concerns
            - Maintains the consultative approach
            
            Customer situation: {situation}
            Discussed benefits: {benefits}"""
        }
    
    async def initialize(self):
        """Initialize the sales handler."""
        self.db = await get_database()
        logger.info("Sales handler initialized")
    
    async def _log_sales_interaction(self, interaction_type: str, customer_id: str, data: Dict[str, Any]):
        """Log sales interaction for tracking and analytics."""
        try:
            interaction_data = {
                "type": interaction_type,
                "customer_id": customer_id,
                "timestamp": datetime.utcnow().isoformat(),
                "data": json.dumps(data)
            }
            
            interaction_key = f"sales_interaction:{customer_id}:{int(time.time())}"
            await self.db.hset(interaction_key, mapping=interaction_data)
            
            # Add to customer's interaction history
            await self.db.lpush(f"customer_interactions:{customer_id}", interaction_key)
            
            # Add to global sales interactions
            await self.db.lpush("sales_interactions", interaction_key)
            
            logger.info(f"SALES: Logged {interaction_type} for customer {customer_id}")
            
        except Exception as e:
            logger.error(f"Error logging sales interaction: {e}")
    
    async def _get_customer_context(self, customer_id: str) -> Dict[str, Any]:
        """Get customer context and history."""
        try:
            # Get customer profile
            customer_data = await self.db.hgetall(f"customer:{customer_id}")
            
            # Get recent interactions
            interaction_keys = await self.db.lrange(f"customer_interactions:{customer_id}", 0, 5)
            recent_interactions = []
            
            for key in interaction_keys:
                interaction = await self.db.hgetall(key)
                if interaction:
                    recent_interactions.append(interaction)
            
            return {
                "profile": customer_data,
                "recent_interactions": recent_interactions,
                "interaction_count": len(interaction_keys)
            }
        
        except Exception as e:
            logger.error(f"Error getting customer context: {e}")
            return {}
    
    async def _update_customer_profile(self, customer_id: str, updates: Dict[str, Any]):
        """Update customer profile with new information."""
        try:
            # Add timestamp to updates
            updates["last_updated"] = datetime.utcnow().isoformat()
            
            await self.db.hset(f"customer:{customer_id}", mapping=updates)
            logger.info(f"Updated customer profile: {customer_id}")
            
        except Exception as e:
            logger.error(f"Error updating customer profile: {e}")
    
    async def process_inquiry(self, inquiry: str, customer_id: str, platform: str, source_id: str = "") -> Optional[str]:
        """Process a sales inquiry and generate appropriate response."""
        try:
            # Get customer context
            context = await self._get_customer_context(customer_id)
            
            # Analyze inquiry sentiment and intent
            sentiment_analysis = await core_ai.analyze_sentiment(inquiry)
            
            # Determine response strategy based on context and sentiment
            if context.get("interaction_count", 0) == 0:
                # First-time customer
                prompt_type = "initial_contact"
                prompt = self.sales_prompts[prompt_type].format(
                    inquiry=inquiry,
                    context="First-time inquiry"
                )
            else:
                # Returning customer
                recent_interactions = context.get("recent_interactions", [])
                last_interaction = recent_interactions[0] if recent_interactions else {}
                
                prompt_type = "follow_up"
                prompt = self.sales_prompts[prompt_type].format(
                    previous_conversation=last_interaction.get("data", ""),
                    customer_response=inquiry,
                    days_since_contact=1  # Simplified for now
                )
            
            # Generate response using AI
            response = await core_ai.generate_content(
                prompt=prompt,
                temperature=0.7
            )
            
            if response:
                # Log the interaction
                await self._log_sales_interaction("inquiry_processed", customer_id, {
                    "inquiry": inquiry,
                    "response": response,
                    "platform": platform,
                    "source_id": source_id,
                    "prompt_type": prompt_type,
                    "sentiment": sentiment_analysis
                })
                
                # Update customer profile
                profile_updates = {
                    "last_contact": datetime.utcnow().isoformat(),
                    "last_platform": platform,
                    "inquiry_count": str(int(context.get("profile", {}).get("inquiry_count", "0")) + 1)
                }
                
                if sentiment_analysis:
                    profile_updates["last_sentiment"] = sentiment_analysis.get("sentiment", "neutral")
                    profile_updates["last_intent"] = sentiment_analysis.get("intent", "question")
                
                await self._update_customer_profile(customer_id, profile_updates)
                
                logger.info(f"SALES: Processed inquiry for customer {customer_id} on {platform}")
                return response
            
            return None
        
        except Exception as e:
            logger.exception(f"Error processing sales inquiry: {e}")
            return None
    
    async def handle_objection(self, objection: str, customer_id: str, context: str = "") -> Optional[str]:
        """Handle customer objections with appropriate responses."""
        try:
            prompt = self.sales_prompts["objection_handling"].format(
                objection=objection,
                context=context
            )
            
            response = await core_ai.generate_content(
                prompt=prompt,
                temperature=0.6
            )
            
            if response:
                await self._log_sales_interaction("objection_handled", customer_id, {
                    "objection": objection,
                    "response": response,
                    "context": context
                })
                
                logger.info(f"SALES: Handled objection for customer {customer_id}")
                return response
            
            return None
        
        except Exception as e:
            logger.exception(f"Error handling objection: {e}")
            return None
    
    async def generate_follow_up(self, customer_id: str, days_since_contact: int = 1) -> Optional[str]:
        """Generate follow-up message for a customer."""
        try:
            context = await self._get_customer_context(customer_id)
            recent_interactions = context.get("recent_interactions", [])
            
            if not recent_interactions:
                logger.warning(f"No previous interactions found for customer {customer_id}")
                return None
            
            last_interaction = recent_interactions[0]
            last_data = json.loads(last_interaction.get("data", "{}"))
            
            prompt = self.sales_prompts["follow_up"].format(
                previous_conversation=last_data.get("inquiry", ""),
                customer_response=last_data.get("response", ""),
                days_since_contact=days_since_contact
            )
            
            response = await core_ai.generate_content(
                prompt=prompt,
                temperature=0.7
            )
            
            if response:
                await self._log_sales_interaction("follow_up_generated", customer_id, {
                    "follow_up_message": response,
                    "days_since_contact": days_since_contact
                })
                
                logger.info(f"SALES: Generated follow-up for customer {customer_id}")
                return response
            
            return None
        
        except Exception as e:
            logger.exception(f"Error generating follow-up: {e}")
            return None
    
    async def create_payment_link(self, customer_id: str, amount: str, description: str, currency: str = "USD") -> Optional[Dict[str, Any]]:
        """Create a payment link for a customer."""
        try:
            # Create PayPal order
            order = await paypal.create_order(
                amount=amount,
                currency=currency,
                description=description
            )
            
            if order:
                # Extract approval URL
                approval_url = None
                for link in order.get("links", []):
                    if link.get("rel") == "approve":
                        approval_url = link.get("href")
                        break
                
                if approval_url:
                    # Log the payment link creation
                    await self._log_sales_interaction("payment_link_created", customer_id, {
                        "order_id": order.get("id"),
                        "amount": amount,
                        "currency": currency,
                        "description": description,
                        "approval_url": approval_url
                    })
                    
                    # Update customer profile
                    await self._update_customer_profile(customer_id, {
                        "last_payment_request": datetime.utcnow().isoformat(),
                        "last_payment_amount": amount,
                        "payment_status": "pending"
                    })
                    
                    logger.info(f"SALES: Created payment link for customer {customer_id} - ${amount}")
                    
                    return {
                        "order_id": order.get("id"),
                        "approval_url": approval_url,
                        "amount": amount,
                        "currency": currency
                    }
            
            return None
        
        except Exception as e:
            logger.exception(f"Error creating payment link: {e}")
            return None
    
    async def send_sales_message(self, customer_id: str, message: str, platform: str, target_id: str) -> bool:
        """Send a sales message via the specified platform."""
        try:
            success = False
            
            if platform == "twitter":
                if target_id.startswith("@"):
                    # Public reply
                    result = await twitter.post_tweet(f"{target_id} {message}")
                    success = result is not None
                else:
                    # DM (would need additional Twitter API setup)
                    logger.warning("Twitter DM functionality not implemented")
                    success = False
            
            elif platform == "mastodon":
                if target_id.startswith("@"):
                    # Public reply
                    result = await mastodon.post_status(f"{target_id} {message}")
                    success = result is not None
                else:
                    logger.warning("Mastodon DM functionality not implemented")
                    success = False
            
            elif platform == "discord":
                if target_id.startswith("channel:"):
                    # Channel message
                    channel_id = target_id.replace("channel:", "")
                    result = await discord.send_message(channel_id, message)
                    success = result is not None
                elif target_id.startswith("user:"):
                    # Direct message
                    user_id = target_id.replace("user:", "")
                    result = await discord.send_dm(user_id, message)
                    success = result is not None
            
            if success:
                await self._log_sales_interaction("message_sent", customer_id, {
                    "message": message,
                    "platform": platform,
                    "target_id": target_id
                })
                
                logger.info(f"SALES: Sent message to customer {customer_id} via {platform}")
            
            return success
        
        except Exception as e:
            logger.exception(f"Error sending sales message: {e}")
            return False
    
    async def get_sales_pipeline(self) -> Dict[str, Any]:
        """Get current sales pipeline status."""
        try:
            # Get all customers with recent activity
            pipeline = {
                "total_customers": 0,
                "active_conversations": 0,
                "pending_payments": 0,
                "completed_sales": 0,
                "pipeline_value": 0.0,
                "customers_by_stage": {
                    "inquiry": 0,
                    "engaged": 0,
                    "negotiating": 0,
                    "closing": 0,
                    "completed": 0
                }
            }
            
            # This would be more complex in a real implementation
            # For now, get basic metrics from Redis
            
            sales_interactions = await self.db.lrange("sales_interactions", 0, -1)
            pipeline["total_customers"] = len(set([
                interaction.split(":")[1] for interaction in sales_interactions
                if len(interaction.split(":")) > 1
            ]))
            
            # Get payment analytics
            payment_analytics = await paypal.get_payment_analytics()
            pipeline["completed_sales"] = payment_analytics.get("total_sales_count", 0)
            pipeline["pipeline_value"] = payment_analytics.get("total_sales_amount", 0.0)
            
            return pipeline
        
        except Exception as e:
            logger.exception(f"Error getting sales pipeline: {e}")
            return {}
    
    async def schedule_follow_ups(self) -> List[Dict[str, Any]]:
        """Schedule follow-up messages for customers."""
        try:
            follow_ups = []
            
            # Get customers who need follow-up (simplified logic)
            # In a real implementation, this would be more sophisticated
            
            # For now, just return empty list as this would be handled by the scheduler
            logger.info("Follow-up scheduling completed")
            return follow_ups
        
        except Exception as e:
            logger.exception(f"Error scheduling follow-ups: {e}")
            return []

# Global sales handler instance
_sales_handler: Optional[SalesHandler] = None

async def get_sales_handler() -> SalesHandler:
    """Get sales handler instance."""
    global _sales_handler
    if _sales_handler is None:
        _sales_handler = SalesHandler()
        await _sales_handler.initialize()
    return _sales_handler

# Convenience functions
async def process_inquiry(inquiry: str, customer_id: str, platform: str, source_id: str = "") -> Optional[str]:
    """Process a sales inquiry."""
    handler = await get_sales_handler()
    return await handler.process_inquiry(inquiry, customer_id, platform, source_id)

async def handle_objection(objection: str, customer_id: str, context: str = "") -> Optional[str]:
    """Handle a customer objection."""
    handler = await get_sales_handler()
    return await handler.handle_objection(objection, customer_id, context)

async def generate_follow_up(customer_id: str, days_since_contact: int = 1) -> Optional[str]:
    """Generate a follow-up message."""
    handler = await get_sales_handler()
    return await handler.generate_follow_up(customer_id, days_since_contact)

async def create_payment_link(customer_id: str, amount: str, description: str, currency: str = "USD") -> Optional[Dict[str, Any]]:
    """Create a payment link."""
    handler = await get_sales_handler()
    return await handler.create_payment_link(customer_id, amount, description, currency)

async def send_sales_message(customer_id: str, message: str, platform: str, target_id: str) -> bool:
    """Send a sales message."""
    handler = await get_sales_handler()
    return await handler.send_sales_message(customer_id, message, platform, target_id)

async def get_sales_pipeline() -> Dict[str, Any]:
    """Get sales pipeline status."""
    handler = await get_sales_handler()
    return await handler.get_sales_pipeline()
