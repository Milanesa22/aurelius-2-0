import asyncio
import aiohttp
import json
import time
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
from config import config
from logging_setup import get_logger
from db.redis_client import get_database

logger = get_logger("mastodon")

class MastodonClient:
    """Mastodon API client for automated posting and engagement."""
    
    def __init__(self):
        self.base_url = config.mastodon_instance_url.rstrip('/')
        self.headers = {
            "Authorization": f"Bearer {config.mastodon_token}",
            "Content-Type": "application/json"
        }
        self.session: Optional[aiohttp.ClientSession] = None
        self.rate_limit_key = "mastodon_rate_limit"
        self.last_post_key = "mastodon_last_post"
    
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
    
    async def _check_rate_limit(self) -> bool:
        """Check if we're within rate limits."""
        try:
            db = await get_database()
            
            # Get current hour's post count
            current_hour = datetime.now().strftime("%Y-%m-%d-%H")
            rate_limit_key = f"{self.rate_limit_key}:{current_hour}"
            
            current_count = await db.get(rate_limit_key)
            current_count = int(current_count) if current_count else 0
            
            if current_count >= config.mastodon_rate_limit:
                logger.warning(f"Mastodon rate limit reached: {current_count}/{config.mastodon_rate_limit}")
                return False
            
            return True
        except Exception as e:
            logger.error(f"Error checking Mastodon rate limit: {e}")
            return False
    
    async def _increment_rate_limit(self):
        """Increment the rate limit counter."""
        try:
            db = await get_database()
            current_hour = datetime.now().strftime("%Y-%m-%d-%H")
            rate_limit_key = f"{self.rate_limit_key}:{current_hour}"
            
            await db.incr(rate_limit_key)
            # Set expiration to 2 hours to clean up old counters
            await db.set(f"{rate_limit_key}_exp", "1", ex=7200)
            
        except Exception as e:
            logger.error(f"Error incrementing Mastodon rate limit: {e}")
    
    async def _log_interaction(self, interaction_type: str, data: Dict[str, Any]):
        """Log interaction to database for analytics."""
        try:
            db = await get_database()
            interaction_data = {
                "type": interaction_type,
                "platform": "mastodon",
                "timestamp": datetime.utcnow().isoformat(),
                "data": json.dumps(data)
            }
            
            interaction_key = f"interaction:mastodon:{int(time.time())}"
            await db.hset(interaction_key, mapping=interaction_data)
            
            # Add to interactions list for analytics
            await db.lpush("interactions:mastodon", interaction_key)
            
        except Exception as e:
            logger.error(f"Error logging Mastodon interaction: {e}")
    
    async def post_status(self, content: str, in_reply_to_id: Optional[str] = None, visibility: str = "public") -> Optional[Dict[str, Any]]:
        """Post a status to Mastodon."""
        if not content or len(content) > 500:
            logger.error(f"Invalid status content length: {len(content) if content else 0}")
            return None
        
        if not await self._check_rate_limit():
            return None
        
        try:
            url = f"{self.base_url}/api/v1/statuses"
            payload = {
                "status": content,
                "visibility": visibility
            }
            
            if in_reply_to_id:
                payload["in_reply_to_id"] = in_reply_to_id
            
            session = await self._get_session()
            
            async with session.post(url, json=payload, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    await self._increment_rate_limit()
                    await self._log_interaction("post_status", {
                        "status_id": data.get("id"),
                        "content": content,
                        "reply_to": in_reply_to_id,
                        "visibility": visibility
                    })
                    
                    logger.info(f"Status posted successfully: {data.get('id')}")
                    return data
                
                elif response.status == 429:
                    logger.warning("Mastodon API rate limit exceeded")
                    return None
                
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to post status ({response.status}): {error_text}")
                    return None
        
        except Exception as e:
            logger.exception(f"Exception posting status: {e}")
            return None
    
    async def get_notifications(self, since_id: Optional[str] = None, types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get notifications (mentions, follows, etc.)."""
        try:
            url = f"{self.base_url}/api/v1/notifications"
            params = {}
            
            if since_id:
                params["since_id"] = since_id
            
            if types:
                params["types[]"] = types
            
            session = await self._get_session()
            
            async with session.get(url, params=params, headers=self.headers) as response:
                if response.status == 200:
                    notifications = await response.json()
                    
                    await self._log_interaction("get_notifications", {
                        "count": len(notifications),
                        "since_id": since_id,
                        "types": types
                    })
                    
                    logger.info(f"Retrieved {len(notifications)} notifications")
                    return notifications
                
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get notifications ({response.status}): {error_text}")
                    return []
        
        except Exception as e:
            logger.exception(f"Exception getting notifications: {e}")
            return []
    
    async def get_mentions(self, since_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get mentions specifically."""
        return await self.get_notifications(since_id=since_id, types=["mention"])
    
    async def reply_to_status(self, status_id: str, content: str) -> Optional[Dict[str, Any]]:
        """Reply to a specific status."""
        return await self.post_status(content, in_reply_to_id=status_id)
    
    async def search_statuses(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search for statuses matching a query."""
        try:
            url = f"{self.base_url}/api/v2/search"
            params = {
                "q": query,
                "type": "statuses",
                "limit": min(limit, 40)  # API limit
            }
            
            session = await self._get_session()
            
            async with session.get(url, params=params, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    statuses = data.get("statuses", [])
                    
                    await self._log_interaction("search_statuses", {
                        "query": query,
                        "count": len(statuses)
                    })
                    
                    logger.info(f"Found {len(statuses)} statuses for query: {query}")
                    return statuses
                
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to search statuses ({response.status}): {error_text}")
                    return []
        
        except Exception as e:
            logger.exception(f"Exception searching statuses: {e}")
            return []
    
    async def get_home_timeline(self, limit: int = 20, since_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get home timeline."""
        try:
            url = f"{self.base_url}/api/v1/timelines/home"
            params = {"limit": min(limit, 40)}
            
            if since_id:
                params["since_id"] = since_id
            
            session = await self._get_session()
            
            async with session.get(url, params=params, headers=self.headers) as response:
                if response.status == 200:
                    timeline = await response.json()
                    
                    logger.info(f"Retrieved {len(timeline)} timeline posts")
                    return timeline
                
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get timeline ({response.status}): {error_text}")
                    return []
        
        except Exception as e:
            logger.exception(f"Exception getting timeline: {e}")
            return []
    
    async def favourite_status(self, status_id: str) -> bool:
        """Favourite (like) a status."""
        try:
            url = f"{self.base_url}/api/v1/statuses/{status_id}/favourite"
            session = await self._get_session()
            
            async with session.post(url, headers=self.headers) as response:
                if response.status == 200:
                    await self._log_interaction("favourite_status", {"status_id": status_id})
                    logger.info(f"Favourited status: {status_id}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to favourite status ({response.status}): {error_text}")
                    return False
        
        except Exception as e:
            logger.exception(f"Exception favouriting status: {e}")
            return False
    
    async def boost_status(self, status_id: str) -> bool:
        """Boost (reblog) a status."""
        try:
            url = f"{self.base_url}/api/v1/statuses/{status_id}/reblog"
            session = await self._get_session()
            
            async with session.post(url, headers=self.headers) as response:
                if response.status == 200:
                    await self._log_interaction("boost_status", {"status_id": status_id})
                    logger.info(f"Boosted status: {status_id}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to boost status ({response.status}): {error_text}")
                    return False
        
        except Exception as e:
            logger.exception(f"Exception boosting status: {e}")
            return False
    
    async def follow_user(self, user_id: str) -> bool:
        """Follow a user."""
        try:
            url = f"{self.base_url}/api/v1/accounts/{user_id}/follow"
            session = await self._get_session()
            
            async with session.post(url, headers=self.headers) as response:
                if response.status == 200:
                    await self._log_interaction("follow_user", {"user_id": user_id})
                    logger.info(f"Followed user: {user_id}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to follow user ({response.status}): {error_text}")
                    return False
        
        except Exception as e:
            logger.exception(f"Exception following user: {e}")
            return False
    
    async def get_account_info(self) -> Optional[Dict[str, Any]]:
        """Get authenticated account information."""
        try:
            url = f"{self.base_url}/api/v1/accounts/verify_credentials"
            session = await self._get_session()
            
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    account_info = await response.json()
                    logger.info(f"Retrieved account info for: {account_info.get('username')}")
                    return account_info
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get account info ({response.status}): {error_text}")
                    return None
        
        except Exception as e:
            logger.exception(f"Exception getting account info: {e}")
            return None
    
    async def get_rate_limit_status(self) -> Dict[str, Any]:
        """Get current rate limit status."""
        try:
            db = await get_database()
            current_hour = datetime.now().strftime("%Y-%m-%d-%H")
            rate_limit_key = f"{self.rate_limit_key}:{current_hour}"
            
            current_count = await db.get(rate_limit_key)
            current_count = int(current_count) if current_count else 0
            
            return {
                "current_count": current_count,
                "limit": config.mastodon_rate_limit,
                "remaining": max(0, config.mastodon_rate_limit - current_count),
                "reset_time": (datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)).isoformat()
            }
        
        except Exception as e:
            logger.error(f"Error getting rate limit status: {e}")
            return {"error": str(e)}

# Global Mastodon client instance
_mastodon_client: Optional[MastodonClient] = None

async def get_mastodon_client() -> MastodonClient:
    """Get Mastodon client instance."""
    global _mastodon_client
    if _mastodon_client is None:
        _mastodon_client = MastodonClient()
    return _mastodon_client

async def close_mastodon_client():
    """Close Mastodon client."""
    global _mastodon_client
    if _mastodon_client:
        await _mastodon_client.close()
        _mastodon_client = None

# Convenience functions
async def post_status(content: str, in_reply_to_id: Optional[str] = None, visibility: str = "public") -> Optional[Dict[str, Any]]:
    """Post a status using the global client."""
    client = await get_mastodon_client()
    return await client.post_status(content, in_reply_to_id, visibility)

async def get_mentions(since_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get mentions using the global client."""
    client = await get_mastodon_client()
    return await client.get_mentions(since_id)

async def reply_to_status(status_id: str, content: str) -> Optional[Dict[str, Any]]:
    """Reply to a status using the global client."""
    client = await get_mastodon_client()
    return await client.reply_to_status(status_id, content)

async def search_statuses(query: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Search statuses using the global client."""
    client = await get_mastodon_client()
    return await client.search_statuses(query, limit)

async def favourite_status(status_id: str) -> bool:
    """Favourite a status using the global client."""
    client = await get_mastodon_client()
    return await client.favourite_status(status_id)

async def boost_status(status_id: str) -> bool:
    """Boost a status using the global client."""
    client = await get_mastodon_client()
    return await client.boost_status(status_id)

async def get_rate_limit_status() -> Dict[str, Any]:
    """Get rate limit status using the global client."""
    client = await get_mastodon_client()
    return await client.get_rate_limit_status()
