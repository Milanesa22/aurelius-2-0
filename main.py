import asyncio
import signal
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from config import config
from logging_setup import get_logger
from db.redis_client import init_database, close_database
from modules import core_ai
from modules.social import twitter, mastodon, discord
from modules.payment import paypal
import sales
import analytics
import auto_learning

logger = get_logger("main")

class AureliusSystem:
    """Main AURELIUS autonomous business management system."""
    
    def __init__(self):
        self.running = False
        self.tasks = []
        self.db = None
        self.shutdown_event = asyncio.Event()
    
    async def initialize(self):
        """Initialize all system components."""
        try:
            logger.info("Initializing AURELIUS system...")
            
            # Initialize database
            self.db = await init_database()
            logger.info("Database initialized")
            
            # Initialize all modules
            await sales.get_sales_handler()
            await analytics.get_analytics()
            await auto_learning.get_learning_module()
            
            logger.info("All modules initialized successfully")
            
        except Exception as e:
            logger.exception(f"Failed to initialize system: {e}")
            raise
    
    async def start_scheduled_tasks(self):
        """Start all scheduled background tasks."""
        try:
            logger.info("Starting scheduled tasks...")
            
            # Social media monitoring and posting
            self.tasks.append(asyncio.create_task(self._social_media_scheduler()))
            
            # Sales follow-up scheduler
            self.tasks.append(asyncio.create_task(self._sales_scheduler()))
            
            # Analytics report generation
            self.tasks.append(asyncio.create_task(self._analytics_scheduler()))
            
            # Auto-learning cycle
            self.tasks.append(asyncio.create_task(self._learning_scheduler()))
            
            # System health monitoring
            self.tasks.append(asyncio.create_task(self._health_monitor()))
            
            logger.info(f"Started {len(self.tasks)} scheduled tasks")
            
        except Exception as e:
            logger.exception(f"Error starting scheduled tasks: {e}")
    
    async def _social_media_scheduler(self):
        """Handle social media posting and engagement."""
        logger.info("Social media scheduler started")
        
        while not self.shutdown_event.is_set():
            try:
                # Check for mentions and respond
                await self._process_social_mentions()
                
                # Generate and post content
                await self._generate_and_post_content()
                
                # Wait for next cycle
                await asyncio.sleep(config.post_interval * 60)  # Convert minutes to seconds
                
            except Exception as e:
                logger.exception(f"Error in social media scheduler: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying
    
    async def _process_social_mentions(self):
        """Process mentions and generate responses."""
        try:
            # Process Twitter mentions
            try:
                mentions = await twitter.get_mentions()
                for mention in mentions:
                    await self._handle_social_mention("twitter", mention)
            except Exception as e:
                logger.error(f"Error processing Twitter mentions: {e}")
            
            # Process Mastodon mentions
            try:
                mentions = await mastodon.get_mentions()
                for mention in mentions:
                    await self._handle_social_mention("mastodon", mention)
            except Exception as e:
                logger.error(f"Error processing Mastodon mentions: {e}")
            
            # Process Discord messages (simplified - would need more complex logic)
            logger.debug("Discord message processing would be implemented here")
            
        except Exception as e:
            logger.exception(f"Error processing social mentions: {e}")
    
    async def _handle_social_mention(self, platform: str, mention: Dict[str, Any]):
        """Handle a single social media mention."""
        try:
            # Extract mention details
            if platform == "twitter":
                author_id = mention.get("author_id", "")
                tweet_id = mention.get("id", "")
                text = mention.get("text", "")
            elif platform == "mastodon":
                author_id = mention.get("account", {}).get("id", "")
                status_id = mention.get("id", "")
                text = mention.get("content", "")
            else:
                return
            
            if not text or not author_id:
                return
            
            # Check if this looks like a sales inquiry
            is_sales_inquiry = any(keyword in text.lower() for keyword in [
                "price", "cost", "buy", "purchase", "service", "help", "info", "demo"
            ])
            
            if is_sales_inquiry:
                # Process as sales inquiry
                response = await sales.process_inquiry(
                    inquiry=text,
                    customer_id=author_id,
                    platform=platform,
                    source_id=tweet_id if platform == "twitter" else status_id
                )
                
                if response:
                    # Send response
                    if platform == "twitter":
                        await twitter.reply_to_tweet(tweet_id, response)
                    elif platform == "mastodon":
                        await mastodon.reply_to_status(status_id, response)
                    
                    logger.info(f"SALES: Responded to inquiry on {platform}")
            else:
                # Generate general response
                response = await core_ai.generate_reply(
                    original_message=text,
                    context=f"Social media mention on {platform}",
                    tone="helpful"
                )
                
                if response:
                    # Send response
                    if platform == "twitter":
                        await twitter.reply_to_tweet(tweet_id, response)
                    elif platform == "mastodon":
                        await mastodon.reply_to_status(status_id, response)
                    
                    logger.info(f"Responded to mention on {platform}")
        
        except Exception as e:
            logger.exception(f"Error handling {platform} mention: {e}")
    
    async def _generate_and_post_content(self):
        """Generate and post content to social media platforms."""
        try:
            # Get learned optimizations
            optimizations = await auto_learning.apply_learned_optimizations("general", "twitter")
            
            # Generate content topics (simplified)
            topics = [
                "business automation tips",
                "social media management insights",
                "productivity improvements",
                "AI-powered business solutions",
                "customer engagement strategies"
            ]
            
            import random
            topic = random.choice(topics)
            
            # Apply learned keywords if available
            if optimizations.get("recommended_keywords"):
                topic += f" {random.choice(optimizations['recommended_keywords'])}"
            
            # Generate content for each platform
            platforms = [
                ("twitter", twitter.post_tweet),
                ("mastodon", mastodon.post_status),
                ("discord", discord.send_webhook_message)
            ]
            
            for platform_name, post_function in platforms:
                try:
                    # Check rate limits
                    if platform_name == "twitter":
                        rate_status = await twitter.get_rate_limit_status()
                    elif platform_name == "mastodon":
                        rate_status = await mastodon.get_rate_limit_status()
                    elif platform_name == "discord":
                        rate_status = await discord.get_rate_limit_status()
                    
                    if rate_status.get("remaining", 0) <= 0:
                        logger.warning(f"Rate limit reached for {platform_name}, skipping")
                        continue
                    
                    # Generate platform-specific content
                    content = await core_ai.generate_social_post(
                        topic=topic,
                        platform=platform_name,
                        style="engaging"
                    )
                    
                    if content:
                        # Post content
                        if platform_name == "discord":
                            success = await post_function(content)
                        else:
                            result = await post_function(content)
                            success = result is not None
                        
                        if success:
                            logger.info(f"Posted content to {platform_name}")
                        else:
                            logger.warning(f"Failed to post content to {platform_name}")
                    
                except Exception as e:
                    logger.error(f"Error posting to {platform_name}: {e}")
            
        except Exception as e:
            logger.exception(f"Error generating and posting content: {e}")
    
    async def _sales_scheduler(self):
        """Handle sales follow-ups and lead nurturing."""
        logger.info("Sales scheduler started")
        
        while not self.shutdown_event.is_set():
            try:
                # Generate follow-ups for customers
                await self._process_sales_follow_ups()
                
                # Wait for next cycle (every 2 hours)
                await asyncio.sleep(7200)
                
            except Exception as e:
                logger.exception(f"Error in sales scheduler: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retrying
    
    async def _process_sales_follow_ups(self):
        """Process sales follow-ups for customers."""
        try:
            # This would be more sophisticated in a real implementation
            # For now, just log that follow-ups are being processed
            logger.info("Processing sales follow-ups...")
            
            # Get sales pipeline
            pipeline = await sales.get_sales_pipeline()
            logger.info(f"Sales pipeline: {pipeline.get('total_customers', 0)} customers, "
                       f"{pipeline.get('completed_sales', 0)} completed sales")
            
        except Exception as e:
            logger.exception(f"Error processing sales follow-ups: {e}")
    
    async def _analytics_scheduler(self):
        """Handle analytics report generation."""
        logger.info("Analytics scheduler started")
        
        while not self.shutdown_event.is_set():
            try:
                # Generate daily report
                daily_report = await analytics.generate_daily_report()
                if daily_report:
                    logger.info("Generated daily analytics report")
                    
                    # Export report
                    json_report = await analytics.export_report_json(daily_report)
                    if json_report:
                        logger.info("Exported daily report as JSON")
                
                # Check if it's time for weekly/monthly reports
                now = datetime.utcnow()
                
                # Weekly report (Mondays)
                if now.weekday() == 0:  # Monday
                    weekly_report = await analytics.generate_weekly_report()
                    if weekly_report:
                        logger.info("Generated weekly analytics report")
                
                # Monthly report (1st of month)
                if now.day == 1:
                    monthly_report = await analytics.generate_monthly_report()
                    if monthly_report:
                        logger.info("Generated monthly analytics report")
                
                # Wait for next cycle
                await asyncio.sleep(config.analytics_interval * 60)
                
            except Exception as e:
                logger.exception(f"Error in analytics scheduler: {e}")
                await asyncio.sleep(3600)  # Wait 1 hour before retrying
    
    async def _learning_scheduler(self):
        """Handle auto-learning cycles."""
        logger.info("Learning scheduler started")
        
        while not self.shutdown_event.is_set():
            try:
                # Run learning cycle
                learning_result = await auto_learning.run_learning_cycle()
                
                if learning_result.get("insights_generated", 0) > 0:
                    logger.info(f"Learning cycle completed: {learning_result['insights_generated']} insights generated")
                else:
                    logger.info("Learning cycle completed: no new insights")
                
                # Wait for next cycle
                await asyncio.sleep(config.learning_interval * 60)
                
            except Exception as e:
                logger.exception(f"Error in learning scheduler: {e}")
                await asyncio.sleep(1800)  # Wait 30 minutes before retrying
    
    async def _health_monitor(self):
        """Monitor system health and performance."""
        logger.info("Health monitor started")
        
        while not self.shutdown_event.is_set():
            try:
                # Get real-time metrics
                metrics = await analytics.get_real_time_metrics()
                
                # Log system status
                logger.info(f"System health: {metrics.get('system_status', 'unknown')}")
                logger.debug(f"Recent activity: {metrics.get('recent_activity', {})}")
                
                # Check for issues
                rate_limits = metrics.get("rate_limits", {})
                for platform, limits in rate_limits.items():
                    remaining = limits.get("remaining", 0)
                    if remaining < 10:
                        logger.warning(f"{platform} rate limit low: {remaining} remaining")
                
                # Wait for next check (every 15 minutes)
                await asyncio.sleep(900)
                
            except Exception as e:
                logger.exception(f"Error in health monitor: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retrying
    
    async def run(self):
        """Run the main system."""
        try:
            self.running = True
            logger.info("AURELIUS system starting...")
            
            # Initialize system
            await self.initialize()
            
            # Start scheduled tasks
            await self.start_scheduled_tasks()
            
            logger.info("AURELIUS system is now running")
            
            # Wait for shutdown signal
            await self.shutdown_event.wait()
            
        except Exception as e:
            logger.exception(f"Critical error in main system: {e}")
            raise
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Gracefully shutdown the system."""
        try:
            logger.info("Shutting down AURELIUS system...")
            
            self.running = False
            self.shutdown_event.set()
            
            # Cancel all tasks
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            
            # Wait for tasks to complete
            if self.tasks:
                await asyncio.gather(*self.tasks, return_exceptions=True)
            
            # Close all connections
            await core_ai.close_ai_client()
            await twitter.close_twitter_client()
            await mastodon.close_mastodon_client()
            await discord.close_discord_client()
            await paypal.close_paypal_client()
            await close_database()
            
            logger.info("AURELIUS system shutdown complete")
            
        except Exception as e:
            logger.exception(f"Error during shutdown: {e}")

# Global system instance
aurelius_system = AureliusSystem()

def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    aurelius_system.shutdown_event.set()

async def main():
    """Main entry point."""
    try:
        # Set up signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Run the system
        await aurelius_system.run()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("System interrupted by user")
    except Exception as e:
        logger.exception(f"Failed to start system: {e}")
        sys.exit(1)
