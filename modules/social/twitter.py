import asyncio
import aiohttp
import json
import time
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
from config import config
from logging_setup import get_logger
from db.redis_client import get_database

logger = get_logger("twitter")

class TwitterClient:
    """Twitter/X API client for automated posting and engagement."""
    
    def __init__(self):
        self.base_url = "https://api.twitter.com/2"
        self.headers = {
            "Authorization": f"Bearer {config.twitter_bearer_token}",
            "Content-Type": "application/json"
        }
        self.session: Optional[aiohttp.ClientSession] = None
        self.rate_limit_key = "twitter_rate_limit"
        self.last_post_key = "twitter_last_post"
    
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
            
            if current_count >= config.twitter_rate_limit:
                logger.warning(f"Twitter rate limit reached: {current_count}/{config.twitter_rate_limit}")
                return False
            
            return True
        except Exception as e:
            logger.error(f"Error checking Twitter rate limit: {e}")
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
            logger.error(f"Error incrementing Twitter rate limit: {e}")
    
    async def _log_interaction(self, interaction_type: str, data: Dict[str, Any]):
        """Log interaction to database for analytics."""
        try:
            db = await get_database()
            interaction_data = {
                "type": interaction_type,
                "platform": "twitter",
                "timestamp": datetime.utcnow().isoformat(),
                "data": json.dumps(data)
            }
            
            interaction_key = f"interaction:twitter:{int(time.time())}"
            await db.hset(interaction_key, mapping=interaction_data)
            
            # Add to interactions list for analytics
            await db.lpush("interactions:twitter", interaction_key)
            
        except Exception as e:
            logger.error(f"Error logging Twitter interaction: {e}")
    
    async def post_tweet(self, content: str, reply_to_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Post a tweet."""
        if not content or len(content) > 280:
            logger.error(f"Invalid tweet content length: {len(content) if content else 0}")
            return None
        
        if not await self._check_rate_limit():
            return None
        
        try:
            url = f"{self.base_url}/tweets"
            payload = {"text": content}
            
            if reply_to_id:
                payload["reply"] = {"in_reply_to_tweet_id": reply_to_id}
            
            session = await self._get_session()
            
            async with session.post(url, json=payload, headers=self.headers) as response:
                if response.status == 201:
                    data = await response.json()
                    tweet_data = data.get("data", {})
                    
                    await self._increment_rate_limit()
                    await self._log_interaction("post_tweet", {
                        "tweet_id": tweet_data.get("id"),
                        "content": content,
                        "reply_to": reply_to_id
                    })
                    
                    logger.info(f"Tweet posted successfully: {tweet_data.get('id')}")
                    return tweet_data
                
                elif response.status == 429:
                    logger.warning("Twitter API rate limit exceeded")
                    return None
                
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to post tweet ({response.status}): {error_text}")
                    return None
        
        except Exception as e:
            logger.exception(f"Exception posting tweet: {e}")
            return None
    
    async def get_mentions(self, since_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get mentions of the authenticated user."""
        try:
            url = f"{self.base_url}/users/me/mentions"
            params = {
                "tweet.fields": "created_at,author_id,conversation_id,in_reply_to_user_id",
                "user.fields": "username,name",
                "expansions": "author_id"
            }
            
            if since_id:
                params["since_id"] = since_id
            
            session = await self._get_session()
            
            async with session.get(url, params=params, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    mentions = data.get("data", [])
                    
                    await self._log_interaction("get_mentions", {
                        "count": len(mentions),
                        "since_id": since_id
                    })
                    
                    logger.info(f"Retrieved {len(mentions)} mentions")
                    return mentions
                
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get mentions ({response.status}): {error_text}")
                    return []
        
        except Exception as e:
            logger.exception(f"Exception getting mentions: {e}")
            return []
    
    async def reply_to_tweet(self, tweet_id: str, content: str) -> Optional[Dict[str, Any]]:
        """Reply to a specific tweet."""
        return await self.post_tweet(content, reply_to_id=tweet_id)
    
    async def search_tweets(self, query: str, max_results: int = 10) -> List[Dict[str, Any]]:
        """Search for tweets matching a query."""
        try:
            url = f"{self.base_url}/tweets/search/recent"
            params = {
                "query": query,
                "max_results": min(max_results, 100),  # API limit
                "tweet.fields": "created_at,author_id,public_metrics",
                "user.fields": "username,name",
                "expansions": "author_id"
            }
            
            session = await self._get_session()
            
            async with session.get(url, params=params, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    tweets = data.get("data", [])
                    
                    await self._log_interaction("search_tweets", {
                        "query": query,
                        "count": len(tweets)
                    })
                    
                    logger.info(f"Found {len(tweets)} tweets for query: {query}")
                    return tweets
                
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to search tweets ({response.status}): {error_text}")
                    return []
        
        except Exception as e:
            logger.exception(f"Exception searching tweets: {e}")
            return []
    
    async def get_user_tweets(self, user_id: str, max_results: int = 10) -> List[Dict[str, Any]]:
        """Get tweets from a specific user."""
        try:
            url = f"{self.base_url}/users/{user_id}/tweets"
            params = {
                "max_results": min(max_results, 100),
                "tweet.fields": "created_at,public_metrics",
                "exclude": "retweets,replies"
            }
            
            session = await self._get_session()
            
            async with session.get(url, params=params, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    tweets = data.get("data", [])
                    
                    logger.info(f"Retrieved {len(tweets)} tweets from user {user_id}")
                    return tweets
                
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get user tweets ({response.status}): {error_text}")
                    return []
        
        except Exception as e:
            logger.exception(f"Exception getting user tweets: {e}")
            return []
    
    async def like_tweet(self, tweet_id: str) -> bool:
        """Like a tweet."""
        try:
            # First get the authenticated user's ID
            me_url = f"{self.base_url}/users/me"
            session = await self._get_session()
            
            async with session.get(me_url, headers=self.headers) as response:
                if response.status != 200:
                    logger.error("Failed to get authenticated user info")
                    return False
                
                user_data = await response.json()
                user_id = user_data.get("data", {}).get("id")
                
                if not user_id:
                    logger.error("Could not get user ID")
                    return False
            
            # Like the tweet
            url = f"{self.base_url}/users/{user_id}/likes"
            payload = {"tweet_id": tweet_id}
            
            async with session.post(url, json=payload, headers=self.headers) as response:
                if response.status == 200:
                    await self._log_interaction("like_tweet", {"tweet_id": tweet_id})
                    logger.info(f"Liked tweet: {tweet_id}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to like tweet ({response.status}): {error_text}")
                    return False
        
        except Exception as e:
            logger.exception(f"Exception liking tweet: {e}")
            return False
    
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
                "limit": config.twitter_rate_limit,
                "remaining": max(0, config.twitter_rate_limit - current_count),
                "reset_time": (datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)).isoformat()
            }
        
        except Exception as e:
            logger.error(f"Error getting rate limit status: {e}")
            return {"error": str(e)}

# Global Twitter client instance
_twitter_client: Optional[TwitterClient] = None

async def get_twitter_client() -> TwitterClient:
    """Get Twitter client instance."""
    global _twitter_client
    if _twitter_client is None:
        _twitter_client = TwitterClient()
    return _twitter_client

async def close_twitter_client():
    """Close Twitter client."""
    global _twitter_client
    if _twitter_client:
        await _twitter_client.close()
        _twitter_client = None

# Convenience functions
async def post_tweet(content: str, reply_to_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Post a tweet using the global client."""
    client = await get_twitter_client()
    return await client.post_tweet(content, reply_to_id)

async def get_mentions(since_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get mentions using the global client."""
    client = await get_twitter_client()
    return await client.get_mentions(since_id)

async def reply_to_tweet(tweet_id: str, content: str) -> Optional[Dict[str, Any]]:
    """Reply to a tweet using the global client."""
    client = await get_twitter_client()
    return await client.reply_to_tweet(tweet_id, content)

async def search_tweets(query: str, max_results: int = 10) -> List[Dict[str, Any]]:
    """Search tweets using the global client."""
    client = await get_twitter_client()
    return await client.search_tweets(query, max_results)

async def like_tweet(tweet_id: str) -> bool:
    """Like a tweet using the global client."""
    client = await get_twitter_client()
    return await client.like_tweet(tweet_id)

async def get_rate_limit_status() -> Dict[str, Any]:
    """Get rate limit status using the global client."""
    client = await get_twitter_client()
    return await client.get_rate_limit_status()
