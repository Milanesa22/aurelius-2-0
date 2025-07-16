import asyncio
import json
import time
from typing import Optional, Dict, List, Any, Tuple
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from config import config
from logging_setup import get_logger
from db.redis_client import get_database
from modules import core_ai

logger = get_logger("auto_learning")

class LearningModule:
    """Auto-learning system for improving content and sales strategies."""
    
    def __init__(self):
        self.db = None
        self.learning_patterns = {
            "content_performance": {},
            "engagement_patterns": {},
            "sales_conversion": {},
            "customer_behavior": {},
            "optimal_timing": {},
            "platform_effectiveness": {}
        }
    
    async def initialize(self):
        """Initialize the learning module."""
        self.db = await get_database()
        await self._load_existing_patterns()
        logger.info("Auto-learning module initialized")
    
    async def _load_existing_patterns(self):
        """Load existing learning patterns from database."""
        try:
            patterns_data = await self.db.get("learning_patterns")
            if patterns_data:
                self.learning_patterns = json.loads(patterns_data)
                logger.info("Loaded existing learning patterns")
            else:
                logger.info("No existing learning patterns found, starting fresh")
        except Exception as e:
            logger.error(f"Error loading learning patterns: {e}")
    
    async def _save_patterns(self):
        """Save learning patterns to database."""
        try:
            await self.db.set("learning_patterns", json.dumps(self.learning_patterns, default=str))
            logger.info("Saved learning patterns to database")
        except Exception as e:
            logger.error(f"Error saving learning patterns: {e}")
    
    async def _analyze_content_performance(self) -> Dict[str, Any]:
        """Analyze which types of content perform best."""
        try:
            platforms = ["twitter", "mastodon", "discord"]
            content_analysis = {
                "high_performing_keywords": [],
                "optimal_content_length": {},
                "best_content_types": {},
                "engagement_triggers": []
            }
            
            for platform in platforms:
                # Get recent interactions
                interaction_keys = await self.db.lrange(f"interactions:{platform}", 0, 100)
                
                content_performance = defaultdict(list)
                content_lengths = []
                keyword_performance = defaultdict(int)
                
                for key in interaction_keys:
                    interaction = await self.db.hgetall(key)
                    if interaction and "data" in interaction:
                        try:
                            data = json.loads(interaction["data"])
                            content = data.get("content", "")
                            interaction_type = interaction.get("type", "")
                            
                            if content:
                                # Track content length
                                content_lengths.append(len(content))
                                
                                # Extract keywords (simplified)
                                words = content.lower().split()
                                for word in words:
                                    if len(word) > 3 and word.isalpha():
                                        keyword_performance[word] += 1
                                
                                # Categorize content type
                                content_type = self._categorize_content(content)
                                
                                # Estimate engagement (simplified - in real implementation, 
                                # this would use actual engagement metrics)
                                engagement_score = self._estimate_engagement(interaction_type, data)
                                content_performance[content_type].append(engagement_score)
                        
                        except json.JSONDecodeError:
                            continue
                
                # Analyze optimal content length
                if content_lengths:
                    content_analysis["optimal_content_length"][platform] = {
                        "average": sum(content_lengths) / len(content_lengths),
                        "median": sorted(content_lengths)[len(content_lengths) // 2],
                        "recommended_range": [
                            int(sum(content_lengths) / len(content_lengths) * 0.8),
                            int(sum(content_lengths) / len(content_lengths) * 1.2)
                        ]
                    }
                
                # Find best performing content types
                if content_performance:
                    type_averages = {
                        content_type: sum(scores) / len(scores)
                        for content_type, scores in content_performance.items()
                        if scores
                    }
                    content_analysis["best_content_types"][platform] = sorted(
                        type_averages.items(), key=lambda x: x[1], reverse=True
                    )[:3]
                
                # Top performing keywords
                top_keywords = sorted(keyword_performance.items(), key=lambda x: x[1], reverse=True)[:10]
                content_analysis["high_performing_keywords"].extend([kw[0] for kw in top_keywords])
            
            # Remove duplicates and get global top keywords
            content_analysis["high_performing_keywords"] = list(set(content_analysis["high_performing_keywords"]))[:15]
            
            return content_analysis
        
        except Exception as e:
            logger.exception(f"Error analyzing content performance: {e}")
            return {}
    
    def _categorize_content(self, content: str) -> str:
        """Categorize content type based on content analysis."""
        content_lower = content.lower()
        
        # Simple categorization rules
        if any(word in content_lower for word in ["buy", "purchase", "sale", "offer", "discount"]):
            return "promotional"
        elif any(word in content_lower for word in ["how", "why", "what", "guide", "tip"]):
            return "educational"
        elif any(word in content_lower for word in ["?", "question", "ask", "help"]):
            return "question"
        elif any(word in content_lower for word in ["thank", "appreciate", "grateful"]):
            return "appreciation"
        elif any(word in content_lower for word in ["new", "update", "announce", "launch"]):
            return "announcement"
        else:
            return "general"
    
    def _estimate_engagement(self, interaction_type: str, data: Dict[str, Any]) -> float:
        """Estimate engagement score based on interaction type and data."""
        # Simplified engagement scoring
        base_scores = {
            "post": 1.0,
            "reply": 2.0,
            "like": 1.5,
            "share": 3.0,
            "mention": 2.5,
            "dm": 4.0
        }
        
        score = 0.0
        for key, value in base_scores.items():
            if key in interaction_type.lower():
                score = value
                break
        
        # Bonus for certain indicators
        if "sales" in str(data).lower():
            score *= 1.5
        if "follow" in str(data).lower():
            score *= 1.3
        
        return score
    
    async def _analyze_timing_patterns(self) -> Dict[str, Any]:
        """Analyze optimal posting times and engagement patterns."""
        try:
            timing_analysis = {
                "optimal_hours": {},
                "optimal_days": {},
                "platform_specific_timing": {}
            }
            
            platforms = ["twitter", "mastodon", "discord"]
            
            for platform in platforms:
                interaction_keys = await self.db.lrange(f"interactions:{platform}", 0, 200)
                
                hour_engagement = defaultdict(list)
                day_engagement = defaultdict(list)
                
                for key in interaction_keys:
                    interaction = await self.db.hgetall(key)
                    if interaction and "timestamp" in interaction:
                        try:
                            timestamp = datetime.fromisoformat(interaction["timestamp"].replace("Z", "+00:00"))
                            hour = timestamp.hour
                            day = timestamp.strftime("%A")
                            
                            # Get engagement score
                            data = json.loads(interaction.get("data", "{}"))
                            engagement_score = self._estimate_engagement(interaction.get("type", ""), data)
                            
                            hour_engagement[hour].append(engagement_score)
                            day_engagement[day].append(engagement_score)
                        
                        except (ValueError, json.JSONDecodeError):
                            continue
                
                # Calculate average engagement by hour
                if hour_engagement:
                    hour_averages = {
                        hour: sum(scores) / len(scores)
                        for hour, scores in hour_engagement.items()
                        if scores
                    }
                    best_hours = sorted(hour_averages.items(), key=lambda x: x[1], reverse=True)[:3]
                    timing_analysis["optimal_hours"][platform] = [hour for hour, _ in best_hours]
                
                # Calculate average engagement by day
                if day_engagement:
                    day_averages = {
                        day: sum(scores) / len(scores)
                        for day, scores in day_engagement.items()
                        if scores
                    }
                    best_days = sorted(day_averages.items(), key=lambda x: x[1], reverse=True)[:3]
                    timing_analysis["optimal_days"][platform] = [day for day, _ in best_days]
                
                timing_analysis["platform_specific_timing"][platform] = {
                    "hour_performance": dict(sorted(hour_averages.items(), key=lambda x: x[1], reverse=True)) if 'hour_averages' in locals() else {},
                    "day_performance": dict(sorted(day_averages.items(), key=lambda x: x[1], reverse=True)) if 'day_averages' in locals() else {}
                }
            
            return timing_analysis
        
        except Exception as e:
            logger.exception(f"Error analyzing timing patterns: {e}")
            return {}
    
    async def _analyze_sales_patterns(self) -> Dict[str, Any]:
        """Analyze sales conversion patterns and customer behavior."""
        try:
            sales_analysis = {
                "conversion_triggers": [],
                "customer_journey_patterns": {},
                "effective_sales_messages": [],
                "objection_handling_success": {}
            }
            
            # Get sales interactions
            sales_keys = await self.db.lrange("sales_interactions", 0, 100)
            
            conversion_events = []
            customer_journeys = defaultdict(list)
            message_effectiveness = defaultdict(list)
            
            for key in sales_keys:
                interaction = await self.db.hgetall(key)
                if interaction:
                    try:
                        customer_id = interaction.get("customer_id", "")
                        interaction_type = interaction.get("type", "")
                        data = json.loads(interaction.get("data", "{}"))
                        timestamp = interaction.get("timestamp", "")
                        
                        # Track customer journey
                        if customer_id:
                            customer_journeys[customer_id].append({
                                "type": interaction_type,
                                "timestamp": timestamp,
                                "data": data
                            })
                        
                        # Analyze message effectiveness
                        if "response" in data:
                            response = data["response"]
                            # Simplified effectiveness scoring
                            effectiveness_score = len(response) / 100  # Basic scoring
                            if "payment" in response.lower():
                                effectiveness_score *= 2
                            if "?" in response:
                                effectiveness_score *= 1.5
                            
                            message_effectiveness[interaction_type].append(effectiveness_score)
                    
                    except json.JSONDecodeError:
                        continue
            
            # Analyze customer journey patterns
            journey_lengths = []
            conversion_paths = []
            
            for customer_id, journey in customer_journeys.items():
                journey_lengths.append(len(journey))
                
                # Check if customer converted (simplified)
                converted = any("payment" in str(step["data"]).lower() for step in journey)
                if converted:
                    conversion_paths.append([step["type"] for step in journey])
            
            if journey_lengths:
                sales_analysis["customer_journey_patterns"] = {
                    "average_touchpoints": sum(journey_lengths) / len(journey_lengths),
                    "conversion_rate": len(conversion_paths) / len(customer_journeys) * 100,
                    "common_conversion_paths": self._find_common_patterns(conversion_paths)
                }
            
            # Find most effective message types
            if message_effectiveness:
                effectiveness_averages = {
                    msg_type: sum(scores) / len(scores)
                    for msg_type, scores in message_effectiveness.items()
                    if scores
                }
                sales_analysis["effective_sales_messages"] = sorted(
                    effectiveness_averages.items(), key=lambda x: x[1], reverse=True
                )[:5]
            
            return sales_analysis
        
        except Exception as e:
            logger.exception(f"Error analyzing sales patterns: {e}")
            return {}
    
    def _find_common_patterns(self, sequences: List[List[str]]) -> List[Dict[str, Any]]:
        """Find common patterns in sequences."""
        if not sequences:
            return []
        
        # Find common subsequences (simplified)
        pattern_counts = Counter()
        
        for sequence in sequences:
            # Generate all possible subsequences of length 2-4
            for length in range(2, min(5, len(sequence) + 1)):
                for i in range(len(sequence) - length + 1):
                    pattern = tuple(sequence[i:i + length])
                    pattern_counts[pattern] += 1
        
        # Return most common patterns
        common_patterns = []
        for pattern, count in pattern_counts.most_common(5):
            if count > 1:  # Only patterns that appear more than once
                common_patterns.append({
                    "pattern": list(pattern),
                    "frequency": count,
                    "percentage": (count / len(sequences)) * 100
                })
        
        return common_patterns
    
    async def _generate_insights(self, analysis_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate actionable insights from analysis results."""
        insights = []
        
        try:
            # Content insights
            content_analysis = analysis_results.get("content_performance", {})
            if content_analysis.get("high_performing_keywords"):
                insights.append({
                    "type": "content_optimization",
                    "priority": "high",
                    "insight": f"Top performing keywords: {', '.join(content_analysis['high_performing_keywords'][:5])}",
                    "action": "Incorporate these keywords more frequently in content",
                    "expected_impact": "15-25% increase in engagement"
                })
            
            # Timing insights
            timing_analysis = analysis_results.get("timing_patterns", {})
            if timing_analysis.get("optimal_hours"):
                for platform, hours in timing_analysis["optimal_hours"].items():
                    if hours:
                        insights.append({
                            "type": "timing_optimization",
                            "priority": "medium",
                            "insight": f"Best posting times for {platform}: {', '.join(map(str, hours))}:00",
                            "action": f"Schedule more posts during these hours on {platform}",
                            "expected_impact": "10-20% increase in engagement"
                        })
            
            # Sales insights
            sales_analysis = analysis_results.get("sales_patterns", {})
            journey_patterns = sales_analysis.get("customer_journey_patterns", {})
            if journey_patterns.get("average_touchpoints"):
                avg_touchpoints = journey_patterns["average_touchpoints"]
                insights.append({
                    "type": "sales_optimization",
                    "priority": "high",
                    "insight": f"Average customer needs {avg_touchpoints:.1f} touchpoints before conversion",
                    "action": "Develop nurture sequences with appropriate follow-up timing",
                    "expected_impact": "20-30% improvement in conversion rate"
                })
            
            # Platform effectiveness insights
            platform_performance = {}
            for platform in ["twitter", "mastodon", "discord"]:
                platform_data = content_analysis.get("best_content_types", {}).get(platform, [])
                if platform_data:
                    best_type = platform_data[0][0] if platform_data else "general"
                    platform_performance[platform] = best_type
            
            if platform_performance:
                insights.append({
                    "type": "platform_optimization",
                    "priority": "medium",
                    "insight": f"Platform content preferences: {platform_performance}",
                    "action": "Tailor content types to each platform's preferences",
                    "expected_impact": "12-18% increase in platform-specific engagement"
                })
            
            return insights
        
        except Exception as e:
            logger.exception(f"Error generating insights: {e}")
            return []
    
    async def _update_strategies(self, insights: List[Dict[str, Any]]):
        """Update system strategies based on insights."""
        try:
            strategy_updates = {
                "content_strategy": {},
                "timing_strategy": {},
                "sales_strategy": {},
                "platform_strategy": {}
            }
            
            for insight in insights:
                insight_type = insight.get("type", "")
                
                if insight_type == "content_optimization":
                    # Update content generation prompts
                    keywords = insight.get("insight", "").split(": ")[-1] if ": " in insight.get("insight", "") else ""
                    if keywords:
                        strategy_updates["content_strategy"]["priority_keywords"] = keywords.split(", ")
                
                elif insight_type == "timing_optimization":
                    # Update posting schedule recommendations
                    platform = insight.get("insight", "").split(" for ")[-1].split(":")[0] if " for " in insight.get("insight", "") else ""
                    times = insight.get("insight", "").split(": ")[-1] if ": " in insight.get("insight", "") else ""
                    if platform and times:
                        strategy_updates["timing_strategy"][platform] = times.split(", ")
                
                elif insight_type == "sales_optimization":
                    # Update sales process
                    if "touchpoints" in insight.get("insight", ""):
                        touchpoints = insight.get("insight", "").split(" needs ")[-1].split(" touchpoints")[0]
                        try:
                            strategy_updates["sales_strategy"]["recommended_touchpoints"] = float(touchpoints)
                        except ValueError:
                            pass
                
                elif insight_type == "platform_optimization":
                    # Update platform-specific strategies
                    strategy_updates["platform_strategy"]["content_preferences"] = insight.get("insight", "")
            
            # Save updated strategies
            await self.db.set("strategy_updates", json.dumps(strategy_updates, default=str))
            
            logger.info(f"Updated strategies based on {len(insights)} insights")
            
        except Exception as e:
            logger.exception(f"Error updating strategies: {e}")
    
    async def run_learning_cycle(self) -> Dict[str, Any]:
        """Run a complete learning cycle."""
        try:
            logger.info("Starting auto-learning cycle")
            
            # Perform various analyses
            analysis_results = {
                "content_performance": await self._analyze_content_performance(),
                "timing_patterns": await self._analyze_timing_patterns(),
                "sales_patterns": await self._analyze_sales_patterns()
            }
            
            # Generate insights
            insights = await self._generate_insights(analysis_results)
            
            # Update strategies
            await self._update_strategies(insights)
            
            # Update learning patterns
            self.learning_patterns.update({
                "last_analysis": datetime.utcnow().isoformat(),
                "analysis_results": analysis_results,
                "generated_insights": insights,
                "cycle_count": self.learning_patterns.get("cycle_count", 0) + 1
            })
            
            # Save patterns
            await self._save_patterns()
            
            learning_summary = {
                "cycle_completed_at": datetime.utcnow().isoformat(),
                "insights_generated": len(insights),
                "analysis_results": analysis_results,
                "insights": insights,
                "improvements_implemented": len([i for i in insights if i.get("priority") == "high"])
            }
            
            logger.info(f"Auto-learning cycle completed. Generated {len(insights)} insights")
            return learning_summary
        
        except Exception as e:
            logger.exception(f"Error in learning cycle: {e}")
            return {"error": str(e)}
    
    async def get_learning_status(self) -> Dict[str, Any]:
        """Get current learning system status."""
        try:
            status = {
                "last_learning_cycle": self.learning_patterns.get("last_analysis", "Never"),
                "total_cycles": self.learning_patterns.get("cycle_count", 0),
                "patterns_learned": len(self.learning_patterns),
                "system_status": "active",
                "next_scheduled_cycle": "Based on scheduler configuration"
            }
            
            # Get recent insights
            recent_insights = self.learning_patterns.get("generated_insights", [])
            status["recent_insights_count"] = len(recent_insights)
            status["high_priority_insights"] = len([i for i in recent_insights if i.get("priority") == "high"])
            
            return status
        
        except Exception as e:
            logger.exception(f"Error getting learning status: {e}")
            return {"error": str(e)}
    
    async def apply_learned_optimizations(self, content_type: str, platform: str) -> Dict[str, Any]:
        """Apply learned optimizations to content generation."""
        try:
            optimizations = {
                "recommended_keywords": [],
                "optimal_length": None,
                "best_posting_time": None,
                "content_style": "general"
            }
            
            # Get learned patterns
            analysis_results = self.learning_patterns.get("analysis_results", {})
            
            # Apply content optimizations
            content_performance = analysis_results.get("content_performance", {})
            if content_performance.get("high_performing_keywords"):
                optimizations["recommended_keywords"] = content_performance["high_performing_keywords"][:5]
            
            # Apply length optimizations
            optimal_lengths = content_performance.get("optimal_content_length", {})
            if platform in optimal_lengths:
                length_data = optimal_lengths[platform]
                optimizations["optimal_length"] = length_data.get("recommended_range", [100, 200])
            
            # Apply timing optimizations
            timing_patterns = analysis_results.get("timing_patterns", {})
            optimal_hours = timing_patterns.get("optimal_hours", {})
            if platform in optimal_hours:
                optimizations["best_posting_time"] = optimal_hours[platform]
            
            # Apply content type optimizations
            best_types = content_performance.get("best_content_types", {})
            if platform in best_types and best_types[platform]:
                optimizations["content_style"] = best_types[platform][0][0]
            
            return optimizations
        
        except Exception as e:
            logger.exception(f"Error applying learned optimizations: {e}")
            return {}

# Global learning module instance
_learning_module: Optional[LearningModule] = None

async def get_learning_module() -> LearningModule:
    """Get learning module instance."""
    global _learning_module
    if _learning_module is None:
        _learning_module = LearningModule()
        await _learning_module.initialize()
    return _learning_module

# Convenience functions
async def run_learning_cycle() -> Dict[str, Any]:
    """Run a learning cycle."""
    module = await get_learning_module()
    return await module.run_learning_cycle()

async def get_learning_status() -> Dict[str, Any]:
    """Get learning system status."""
    module = await get_learning_module()
    return await module.get_learning_status()

async def apply_learned_optimizations(content_type: str, platform: str) -> Dict[str, Any]:
    """Apply learned optimizations."""
    module = await get_learning_module()
    return await module.apply_learned_optimizations(content_type, platform)
