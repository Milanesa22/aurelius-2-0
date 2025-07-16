import asyncio
import aiohttp
import json
import time
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
from config import config
from logging_setup import get_logger
from db.redis_client import get_database

logger = get_logger("discord")

class DiscordClient:
    """Discord API client for automated posting and engagement."""
    
    def __init__(self):
        self.base_url = "https://discord.com/api/v10"
        self.headers = {
            "Authorization": f"Bot {config.discord_token}",
            "Content-Type": "application/json"
        }
        self.webhook_url = config.discord_webhook_url
        self.session: Optional[aiohttp.ClientSession] = None
        self.rate_limit_key = "discord_rate_limit"
        self.last_post_key = "discord_last_post"
    
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
            
            if current_count >= config.discord_rate_limit:
                logger.warning(f"Discord rate limit reached: {current_count}/{config.discord_rate_limit}")
                return False
            
            return True
        except Exception as e:
            logger.error(f"Error checking Discord rate limit: {e}")
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
            logger.error(f"Error incrementing Discord rate limit: {e}")
    
    async def _log_interaction(self, interaction_type: str, data: Dict[str, Any]):
        """Log interaction to database for analytics."""
        try:
            db = await get_database()
            interaction_data = {
                "type": interaction_type,
                "platform": "discord",
                "timestamp": datetime.utcnow().isoformat(),
                "data": json.dumps(data)
            }
            
            interaction_key = f"interaction:discord:{int(time.time())}"
            await db.hset(interaction_key, mapping=interaction_data)
            
            # Add to interactions list for analytics
            await db.lpush("interactions:discord", interaction_key)
            
        except Exception as e:
            logger.error(f"Error logging Discord interaction: {e}")
    
    async def send_webhook_message(self, content: str, username: Optional[str] = None, avatar_url: Optional[str] = None) -> bool:
        """Send a message via Discord webhook."""
        if not content or len(content) > 2000:
            logger.error(f"Invalid message content length: {len(content) if content else 0}")
            return False
        
        if not await self._check_rate_limit():
            return False
        
        try:
            payload = {"content": content}
            
            if username:
                payload["username"] = username
            if avatar_url:
                payload["avatar_url"] = avatar_url
            
            session = await self._get_session()
            
            async with session.post(self.webhook_url, json=payload) as response:
                if response.status == 204:
                    await self._increment_rate_limit()
                    await self._log_interaction("send_webhook_message", {
                        "content": content[:100] + "..." if len(content) > 100 else content,
                        "username": username
                    })
                    
                    logger.info("Discord webhook message sent successfully")
                    return True
                
                elif response.status == 429:
                    logger.warning("Discord webhook rate limit exceeded")
                    return False
                
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to send webhook message ({response.status}): {error_text}")
                    return False
        
        except Exception as e:
            logger.exception(f"Exception sending webhook message: {e}")
            return False
    
    async def send_message(self, channel_id: str, content: str, reply_to: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Send a message to a Discord channel."""
        if not content or len(content) > 2000:
            logger.error(f"Invalid message content length: {len(content) if content else 0}")
            return None
        
        if not await self._check_rate_limit():
            return None
        
        try:
            url = f"{self.base_url}/channels/{channel_id}/messages"
            payload = {"content": content}
            
            if reply_to:
                payload["message_reference"] = {"message_id": reply_to}
            
            session = await self._get_session()
            
            async with session.post(url, json=payload, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    await self._increment_rate_limit()
                    await self._log_interaction("send_message", {
                        "channel_id": channel_id,
                        "message_id": data.get("id"),
                        "content": content[:100] + "..." if len(content) > 100 else content,
                        "reply_to": reply_to
                    })
                    
                    logger.info(f"Discord message sent successfully: {data.get('id')}")
                    return data
                
                elif response.status == 429:
                    logger.warning("Discord API rate limit exceeded")
                    return None
                
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to send message ({response.status}): {error_text}")
                    return None
        
        except Exception as e:
            logger.exception(f"Exception sending message: {e}")
            return None
    
    async def get_channel_messages(self, channel_id: str, limit: int = 50, before: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get messages from a Discord channel."""
        try:
            url = f"{self.base_url}/channels/{channel_id}/messages"
            params = {"limit": min(limit, 100)}  # API limit
            
            if before:
                params["before"] = before
            
            session = await self._get_session()
            
            async with session.get(url, params=params, headers=self.headers) as response:
                if response.status == 200:
                    messages = await response.json()
                    
                    await self._log_interaction("get_channel_messages", {
                        "channel_id": channel_id,
                        "count": len(messages)
                    })
                    
                    logger.info(f"Retrieved {len(messages)} messages from channel {channel_id}")
                    return messages
                
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get messages ({response.status}): {error_text}")
                    return []
        
        except Exception as e:
            logger.exception(f"Exception getting messages: {e}")
            return []
    
    async def reply_to_message(self, channel_id: str, message_id: str, content: str) -> Optional[Dict[str, Any]]:
        """Reply to a specific message."""
        return await self.send_message(channel_id, content, reply_to=message_id)
    
    async def add_reaction(self, channel_id: str, message_id: str, emoji: str) -> bool:
        """Add a reaction to a message."""
        try:
            # URL encode the emoji
            import urllib.parse
            encoded_emoji = urllib.parse.quote(emoji)
            
            url = f"{self.base_url}/channels/{channel_id}/messages/{message_id}/reactions/{encoded_emoji}/@me"
            session = await self._get_session()
            
            async with session.put(url, headers=self.headers) as response:
                if response.status == 204:
                    await self._log_interaction("add_reaction", {
                        "channel_id": channel_id,
                        "message_id": message_id,
                        "emoji": emoji
                    })
                    
                    logger.info(f"Added reaction {emoji} to message {message_id}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to add reaction ({response.status}): {error_text}")
                    return False
        
        except Exception as e:
            logger.exception(f"Exception adding reaction: {e}")
            return False
    
    async def get_guild_channels(self, guild_id: str) -> List[Dict[str, Any]]:
        """Get channels in a guild."""
        try:
            url = f"{self.base_url}/guilds/{guild_id}/channels"
            session = await self._get_session()
            
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    channels = await response.json()
                    logger.info(f"Retrieved {len(channels)} channels from guild {guild_id}")
                    return channels
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get guild channels ({response.status}): {error_text}")
                    return []
        
        except Exception as e:
            logger.exception(f"Exception getting guild channels: {e}")
            return []
    
    async def get_user_info(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a user."""
        try:
            url = f"{self.base_url}/users/{user_id}"
            session = await self._get_session()
            
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    user_info = await response.json()
                    logger.info(f"Retrieved user info for: {user_info.get('username')}")
                    return user_info
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get user info ({response.status}): {error_text}")
                    return None
        
        except Exception as e:
            logger.exception(f"Exception getting user info: {e}")
            return None
    
    async def search_messages(self, channel_id: str, query: str, limit: int = 25) -> List[Dict[str, Any]]:
        """Search for messages in a channel."""
        try:
            url = f"{self.base_url}/channels/{channel_id}/messages/search"
            params = {
                "content": query,
                "limit": min(limit, 25)  # API limit
            }
            
            session = await self._get_session()
            
            async with session.get(url, params=params, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    messages = data.get("messages", [])
                    
                    await self._log_interaction("search_messages", {
                        "channel_id": channel_id,
                        "query": query,
                        "count": len(messages)
                    })
                    
                    logger.info(f"Found {len(messages)} messages for query: {query}")
                    return messages
                
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to search messages ({response.status}): {error_text}")
                    return []
        
        except Exception as e:
            logger.exception(f"Exception searching messages: {e}")
            return []
    
    async def create_dm_channel(self, user_id: str) -> Optional[str]:
        """Create a DM channel with a user."""
        try:
            url = f"{self.base_url}/users/@me/channels"
            payload = {"recipient_id": user_id}
            
            session = await self._get_session()
            
            async with session.post(url, json=payload, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    channel_id = data.get("id")
                    
                    logger.info(f"Created DM channel: {channel_id}")
                    return channel_id
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to create DM channel ({response.status}): {error_text}")
                    return None
        
        except Exception as e:
            logger.exception(f"Exception creating DM channel: {e}")
            return None
    
    async def send_dm(self, user_id: str, content: str) -> Optional[Dict[str, Any]]:
        """Send a direct message to a user."""
        channel_id = await self.create_dm_channel(user_id)
        if channel_id:
            return await self.send_message(channel_id, content)
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
                "limit": config.discord_rate_limit,
                "remaining": max(0, config.discord_rate_limit - current_count),
                "reset_time": (datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)).isoformat()
            }
        
        except Exception as e:
            logger.error(f"Error getting rate limit status: {e}")
            return {"error": str(e)}

# Global Discord client instance
_discord_client: Optional[DiscordClient] = None

async def get_discord_client() -> DiscordClient:
    """Get Discord client instance."""
    global _discord_client
    if _discord_client is None:
        _discord_client = DiscordClient()
    return _discord_client

async def close_discord_client():
    """Close Discord client."""
    global _discord_client
    if _discord_client:
        await _discord_client.close()
        _discord_client = None

# Convenience functions
async def send_webhook_message(content: str, username: Optional[str] = None, avatar_url: Optional[str] = None) -> bool:
    """Send a webhook message using the global client."""
    client = await get_discord_client()
    return await client.send_webhook_message(content, username, avatar_url)

async def send_message(channel_id: str, content: str, reply_to: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Send a message using the global client."""
    client = await get_discord_client()
    return await client.send_message(channel_id, content, reply_to)

async def get_channel_messages(channel_id: str, limit: int = 50, before: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get channel messages using the global client."""
    client = await get_discord_client()
    return await client.get_channel_messages(channel_id, limit, before)

async def reply_to_message(channel_id: str, message_id: str, content: str) -> Optional[Dict[str, Any]]:
    """Reply to a message using the global client."""
    client = await get_discord_client()
    return await client.reply_to_message(channel_id, message_id, content)

async def add_reaction(channel_id: str, message_id: str, emoji: str) -> bool:
    """Add a reaction using the global client."""
    client = await get_discord_client()
    return await client.add_reaction(channel_id, message_id, emoji)

async def send_dm(user_id: str, content: str) -> Optional[Dict[str, Any]]:
    """Send a DM using the global client."""
    client = await get_discord_client()
    return await client.send_dm(user_id, content)

async def get_rate_limit_status() -> Dict[str, Any]:
    """Get rate limit status using the global client."""
    client = await get_discord_client()
    return await client.get_rate_limit_status()
