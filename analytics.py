import asyncio
import json
import time
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
from collections import defaultdict
from config import config
from logging_setup import get_logger
from db.redis_client import get_database
from modules.payment import paypal

logger = get_logger("analytics")

class Analytics:
    """Analytics and reporting system for AURELIUS."""
    
    def __init__(self):
        self.db = None
    
    async def initialize(self):
        """Initialize the analytics system."""
        self.db = await get_database()
        logger.info("Analytics system initialized")
    
    def _get_date_range(self, period: str) -> tuple[datetime, datetime]:
        """Get date range for the specified period."""
        now = datetime.utcnow()
        
        if period == "daily":
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
        elif period == "weekly":
            days_since_monday = now.weekday()
            start_date = (now - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=7)
        elif period == "monthly":
            start_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if now.month == 12:
                end_date = start_date.replace(year=now.year + 1, month=1)
            else:
                end_date = start_date.replace(month=now.month + 1)
        else:
            # Default to daily
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
        
        return start_date, end_date
    
    async def _get_interactions_in_period(self, platform: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get interactions for a platform within a date range."""
        try:
            interaction_keys = await self.db.lrange(f"interactions:{platform}", 0, -1)
            interactions = []
            
            for key in interaction_keys:
                interaction = await self.db.hgetall(key)
                if interaction and "timestamp" in interaction:
                    try:
                        interaction_time = datetime.fromisoformat(interaction["timestamp"].replace("Z", "+00:00"))
                        if start_date <= interaction_time < end_date:
                            interaction["data"] = json.loads(interaction.get("data", "{}"))
                            interactions.append(interaction)
                    except (ValueError, json.JSONDecodeError) as e:
                        logger.warning(f"Error parsing interaction {key}: {e}")
                        continue
            
            return interactions
        
        except Exception as e:
            logger.error(f"Error getting {platform} interactions: {e}")
            return []
    
    async def _get_sales_data_in_period(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Get sales data within a date range."""
        try:
            # Get payment events
            payment_keys = await self.db.lrange("payment_events:paypal", 0, -1)
            sales_data = {
                "total_sales": 0,
                "total_revenue": 0.0,
                "total_refunds": 0,
                "refund_amount": 0.0,
                "orders_created": 0,
                "orders_completed": 0,
                "conversion_events": []
            }
            
            for key in payment_keys:
                event = await self.db.hgetall(key)
                if event and "timestamp" in event:
                    try:
                        event_time = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
                        if start_date <= event_time < end_date:
                            event_data = json.loads(event.get("data", "{}"))
                            event_type = event.get("type", "")
                            
                            if "payment_completed" in event_type:
                                sales_data["total_sales"] += 1
                                amount = float(event_data.get("amount", 0))
                                sales_data["total_revenue"] += amount
                                sales_data["conversion_events"].append({
                                    "type": "sale_completed",
                                    "amount": amount,
                                    "timestamp": event["timestamp"]
                                })
                            
                            elif "order_created" in event_type:
                                sales_data["orders_created"] += 1
                            
                            elif "payment_refunded" in event_type:
                                sales_data["total_refunds"] += 1
                                refund_amount = float(event_data.get("amount", 0))
                                sales_data["refund_amount"] += refund_amount
                    
                    except (ValueError, json.JSONDecodeError) as e:
                        logger.warning(f"Error parsing payment event {key}: {e}")
                        continue
            
            # Calculate net revenue
            sales_data["net_revenue"] = sales_data["total_revenue"] - sales_data["refund_amount"]
            
            # Calculate conversion rate (simplified)
            if sales_data["orders_created"] > 0:
                sales_data["conversion_rate"] = (sales_data["total_sales"] / sales_data["orders_created"]) * 100
            else:
                sales_data["conversion_rate"] = 0.0
            
            return sales_data
        
        except Exception as e:
            logger.error(f"Error getting sales data: {e}")
            return {}
    
    async def _get_engagement_metrics(self, platform: str, interactions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate engagement metrics for a platform."""
        try:
            metrics = {
                "total_interactions": len(interactions),
                "posts_created": 0,
                "replies_sent": 0,
                "mentions_received": 0,
                "likes_given": 0,
                "shares_made": 0,
                "engagement_rate": 0.0,
                "response_time_avg": 0.0,
                "top_performing_content": []
            }
            
            response_times = []
            content_performance = defaultdict(int)
            
            for interaction in interactions:
                interaction_type = interaction.get("type", "")
                data = interaction.get("data", {})
                
                # Count different types of interactions
                if "post" in interaction_type or "tweet" in interaction_type or "status" in interaction_type:
                    metrics["posts_created"] += 1
                elif "reply" in interaction_type:
                    metrics["replies_sent"] += 1
                elif "mention" in interaction_type:
                    metrics["mentions_received"] += 1
                elif "like" in interaction_type or "favourite" in interaction_type:
                    metrics["likes_given"] += 1
                elif "boost" in interaction_type or "retweet" in interaction_type:
                    metrics["shares_made"] += 1
                
                # Track content performance (simplified)
                content = data.get("content", "")
                if content:
                    content_key = content[:50] + "..." if len(content) > 50 else content
                    content_performance[content_key] += 1
            
            # Calculate engagement rate (simplified)
            if metrics["posts_created"] > 0:
                total_engagements = metrics["replies_sent"] + metrics["likes_given"] + metrics["shares_made"]
                metrics["engagement_rate"] = (total_engagements / metrics["posts_created"]) * 100
            
            # Get top performing content
            metrics["top_performing_content"] = [
                {"content": content, "interactions": count}
                for content, count in sorted(content_performance.items(), key=lambda x: x[1], reverse=True)[:5]
            ]
            
            return metrics
        
        except Exception as e:
            logger.error(f"Error calculating engagement metrics: {e}")
            return {}
    
    async def _get_lead_metrics(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Get lead generation metrics."""
        try:
            # Get sales interactions
            sales_keys = await self.db.lrange("sales_interactions", 0, -1)
            
            metrics = {
                "total_leads": 0,
                "new_customers": 0,
                "qualified_leads": 0,
                "conversion_rate": 0.0,
                "lead_sources": defaultdict(int),
                "lead_quality_score": 0.0
            }
            
            unique_customers = set()
            qualified_leads = 0
            
            for key in sales_keys:
                interaction = await self.db.hgetall(key)
                if interaction and "timestamp" in interaction:
                    try:
                        interaction_time = datetime.fromisoformat(interaction["timestamp"].replace("Z", "+00:00"))
                        if start_date <= interaction_time < end_date:
                            customer_id = interaction.get("customer_id", "")
                            if customer_id:
                                unique_customers.add(customer_id)
                                
                                # Check if this is a qualified lead (has multiple interactions)
                                customer_interactions = await self.db.lrange(f"customer_interactions:{customer_id}", 0, -1)
                                if len(customer_interactions) > 1:
                                    qualified_leads += 1
                                
                                # Track lead source
                                data = json.loads(interaction.get("data", "{}"))
                                platform = data.get("platform", "unknown")
                                metrics["lead_sources"][platform] += 1
                    
                    except (ValueError, json.JSONDecodeError) as e:
                        logger.warning(f"Error parsing sales interaction {key}: {e}")
                        continue
            
            metrics["total_leads"] = len(unique_customers)
            metrics["new_customers"] = len(unique_customers)  # Simplified
            metrics["qualified_leads"] = qualified_leads
            
            # Calculate conversion rate
            if metrics["total_leads"] > 0:
                # Get completed sales count for the period
                sales_data = await self._get_sales_data_in_period(start_date, end_date)
                completed_sales = sales_data.get("total_sales", 0)
                metrics["conversion_rate"] = (completed_sales / metrics["total_leads"]) * 100
            
            # Calculate lead quality score (simplified)
            if metrics["total_leads"] > 0:
                metrics["lead_quality_score"] = (qualified_leads / metrics["total_leads"]) * 100
            
            # Convert defaultdict to regular dict for JSON serialization
            metrics["lead_sources"] = dict(metrics["lead_sources"])
            
            return metrics
        
        except Exception as e:
            logger.error(f"Error getting lead metrics: {e}")
            return {}
    
    async def generate_report(self, period: str = "daily") -> Dict[str, Any]:
        """Generate comprehensive analytics report."""
        try:
            start_date, end_date = self._get_date_range(period)
            
            logger.info(f"Generating {period} analytics report for {start_date.date()} to {end_date.date()}")
            
            # Initialize report structure
            report = {
                "period": period,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "generated_at": datetime.utcnow().isoformat(),
                "summary": {},
                "social_media": {},
                "sales": {},
                "leads": {},
                "performance_insights": []
            }
            
            # Get social media analytics for each platform
            platforms = ["twitter", "mastodon", "discord"]
            
            for platform in platforms:
                interactions = await self._get_interactions_in_period(platform, start_date, end_date)
                engagement_metrics = await self._get_engagement_metrics(platform, interactions)
                report["social_media"][platform] = engagement_metrics
            
            # Get sales analytics
            sales_data = await self._get_sales_data_in_period(start_date, end_date)
            report["sales"] = sales_data
            
            # Get lead analytics
            lead_metrics = await self._get_lead_metrics(start_date, end_date)
            report["leads"] = lead_metrics
            
            # Generate summary
            total_interactions = sum(
                report["social_media"][platform].get("total_interactions", 0)
                for platform in platforms
            )
            
            report["summary"] = {
                "total_social_interactions": total_interactions,
                "total_posts_created": sum(
                    report["social_media"][platform].get("posts_created", 0)
                    for platform in platforms
                ),
                "total_sales": sales_data.get("total_sales", 0),
                "total_revenue": sales_data.get("total_revenue", 0.0),
                "total_leads": lead_metrics.get("total_leads", 0),
                "conversion_rate": lead_metrics.get("conversion_rate", 0.0)
            }
            
            # Generate performance insights
            insights = []
            
            # Revenue insight
            if sales_data.get("total_revenue", 0) > 0:
                insights.append({
                    "type": "revenue",
                    "message": f"Generated ${sales_data['total_revenue']:.2f} in revenue with {sales_data['total_sales']} completed sales",
                    "metric": sales_data["total_revenue"]
                })
            
            # Engagement insight
            best_platform = max(platforms, key=lambda p: report["social_media"][p].get("total_interactions", 0))
            best_platform_interactions = report["social_media"][best_platform].get("total_interactions", 0)
            if best_platform_interactions > 0:
                insights.append({
                    "type": "engagement",
                    "message": f"{best_platform.title()} had the highest engagement with {best_platform_interactions} interactions",
                    "metric": best_platform_interactions
                })
            
            # Conversion insight
            if lead_metrics.get("conversion_rate", 0) > 0:
                insights.append({
                    "type": "conversion",
                    "message": f"Lead to sale conversion rate: {lead_metrics['conversion_rate']:.1f}%",
                    "metric": lead_metrics["conversion_rate"]
                })
            
            report["performance_insights"] = insights
            
            # Store report in database
            report_key = f"analytics_report:{period}:{start_date.strftime('%Y-%m-%d')}"
            await self.db.set(report_key, json.dumps(report), ex=86400 * 30)  # Store for 30 days
            
            logger.info(f"Generated {period} analytics report successfully")
            return report
        
        except Exception as e:
            logger.exception(f"Error generating analytics report: {e}")
            return {}
    
    async def get_real_time_metrics(self) -> Dict[str, Any]:
        """Get real-time system metrics."""
        try:
            metrics = {
                "timestamp": datetime.utcnow().isoformat(),
                "system_status": "operational",
                "active_connections": 0,
                "recent_activity": {
                    "last_hour_posts": 0,
                    "last_hour_interactions": 0,
                    "last_hour_sales": 0
                },
                "rate_limits": {},
                "database_status": "connected" if self.db else "disconnected"
            }
            
            # Get rate limit status for each platform
            try:
                from modules.social import twitter, mastodon, discord
                metrics["rate_limits"]["twitter"] = await twitter.get_rate_limit_status()
                metrics["rate_limits"]["mastodon"] = await mastodon.get_rate_limit_status()
                metrics["rate_limits"]["discord"] = await discord.get_rate_limit_status()
            except Exception as e:
                logger.warning(f"Error getting rate limit status: {e}")
            
            # Get recent activity (last hour)
            one_hour_ago = datetime.utcnow() - timedelta(hours=1)
            
            # Count recent interactions
            platforms = ["twitter", "mastodon", "discord"]
            total_recent_interactions = 0
            
            for platform in platforms:
                recent_interactions = await self._get_interactions_in_period(platform, one_hour_ago, datetime.utcnow())
                total_recent_interactions += len(recent_interactions)
                
                # Count posts specifically
                posts = [i for i in recent_interactions if "post" in i.get("type", "")]
                metrics["recent_activity"]["last_hour_posts"] += len(posts)
            
            metrics["recent_activity"]["last_hour_interactions"] = total_recent_interactions
            
            # Count recent sales
            recent_sales = await self._get_sales_data_in_period(one_hour_ago, datetime.utcnow())
            metrics["recent_activity"]["last_hour_sales"] = recent_sales.get("total_sales", 0)
            
            return metrics
        
        except Exception as e:
            logger.exception(f"Error getting real-time metrics: {e}")
            return {"error": str(e)}
    
    async def export_report_json(self, report: Dict[str, Any], filename: Optional[str] = None) -> str:
        """Export report as JSON file."""
        try:
            if not filename:
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                filename = f"aurelius_report_{report.get('period', 'unknown')}_{timestamp}.json"
            
            # Pretty print JSON
            json_content = json.dumps(report, indent=2, default=str)
            
            # In a real implementation, you might save to file system or cloud storage
            # For now, just return the JSON content
            logger.info(f"Exported report as JSON: {filename}")
            return json_content
        
        except Exception as e:
            logger.exception(f"Error exporting report: {e}")
            return ""
    
    async def get_historical_trends(self, days: int = 30) -> Dict[str, Any]:
        """Get historical trends over specified number of days."""
        try:
            trends = {
                "period_days": days,
                "daily_metrics": [],
                "trends": {
                    "revenue_trend": "stable",
                    "engagement_trend": "stable",
                    "lead_trend": "stable"
                }
            }
            
            # Get daily metrics for the past N days
            for i in range(days):
                date = datetime.utcnow() - timedelta(days=i)
                start_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = start_date + timedelta(days=1)
                
                # Get basic metrics for this day
                sales_data = await self._get_sales_data_in_period(start_date, end_date)
                lead_data = await self._get_lead_metrics(start_date, end_date)
                
                daily_metric = {
                    "date": start_date.strftime("%Y-%m-%d"),
                    "revenue": sales_data.get("total_revenue", 0.0),
                    "sales": sales_data.get("total_sales", 0),
                    "leads": lead_data.get("total_leads", 0)
                }
                
                trends["daily_metrics"].append(daily_metric)
            
            # Analyze trends (simplified)
            if len(trends["daily_metrics"]) >= 7:
                recent_week = trends["daily_metrics"][:7]
                previous_week = trends["daily_metrics"][7:14] if len(trends["daily_metrics"]) >= 14 else []
                
                if previous_week:
                    recent_revenue = sum(d["revenue"] for d in recent_week)
                    previous_revenue = sum(d["revenue"] for d in previous_week)
                    
                    if recent_revenue > previous_revenue * 1.1:
                        trends["trends"]["revenue_trend"] = "increasing"
                    elif recent_revenue < previous_revenue * 0.9:
                        trends["trends"]["revenue_trend"] = "decreasing"
            
            return trends
        
        except Exception as e:
            logger.exception(f"Error getting historical trends: {e}")
            return {}

# Global analytics instance
_analytics: Optional[Analytics] = None

async def get_analytics() -> Analytics:
    """Get analytics instance."""
    global _analytics
    if _analytics is None:
        _analytics = Analytics()
        await _analytics.initialize()
    return _analytics

# Convenience functions
async def generate_daily_report() -> Dict[str, Any]:
    """Generate daily analytics report."""
    analytics = await get_analytics()
    return await analytics.generate_report("daily")

async def generate_weekly_report() -> Dict[str, Any]:
    """Generate weekly analytics report."""
    analytics = await get_analytics()
    return await analytics.generate_report("weekly")

async def generate_monthly_report() -> Dict[str, Any]:
    """Generate monthly analytics report."""
    analytics = await get_analytics()
    return await analytics.generate_report("monthly")

async def get_real_time_metrics() -> Dict[str, Any]:
    """Get real-time system metrics."""
    analytics = await get_analytics()
    return await analytics.get_real_time_metrics()

async def export_report_json(report: Dict[str, Any], filename: Optional[str] = None) -> str:
    """Export report as JSON."""
    analytics = await get_analytics()
    return await analytics.export_report_json(report, filename)

async def get_historical_trends(days: int = 30) -> Dict[str, Any]:
    """Get historical trends."""
    analytics = await get_analytics()
    return await analytics.get_historical_trends(days)
