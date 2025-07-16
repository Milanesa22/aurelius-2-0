import asyncio
import json
import aiofiles
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from redis.asyncio import Redis
from config import config
from logging_setup import get_logger

logger = get_logger("redis_client")

class LocalStorageFallback:
    """Local file-based storage fallback when Redis is unavailable."""
    
    def __init__(self, storage_dir: str = "local_storage"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        logger.warning("Using local storage fallback - data persistence may be degraded")
    
    def _get_file_path(self, key: str) -> Path:
        """Get file path for a given key."""
        safe_key = key.replace(":", "_").replace("/", "_")
        return self.storage_dir / f"{safe_key}.json"
    
    async def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """Set a key-value pair."""
        try:
            file_path = self._get_file_path(key)
            data = {
                "value": value,
                "expires_at": None if ex is None else (asyncio.get_event_loop().time() + ex)
            }
            async with aiofiles.open(file_path, 'w') as f:
                await f.write(json.dumps(data, default=str))
            return True
        except Exception as e:
            logger.error(f"Failed to set key {key} in local storage: {e}")
            return False
    
    async def get(self, key: str) -> Optional[str]:
        """Get value by key."""
        try:
            file_path = self._get_file_path(key)
            if not file_path.exists():
                return None
            
            async with aiofiles.open(file_path, 'r') as f:
                content = await f.read()
                data = json.loads(content)
            
            # Check expiration
            if data.get("expires_at") and asyncio.get_event_loop().time() > data["expires_at"]:
                await self.delete(key)
                return None
            
            return data["value"]
        except Exception as e:
            logger.error(f"Failed to get key {key} from local storage: {e}")
            return None
    
    async def hset(self, key: str, mapping: Dict[str, Any]) -> int:
        """Set hash fields."""
        try:
            existing = await self.hgetall(key) or {}
            existing.update(mapping)
            await self.set(key, existing)
            return len(mapping)
        except Exception as e:
            logger.error(f"Failed to hset key {key} in local storage: {e}")
            return 0
    
    async def hget(self, key: str, field: str) -> Optional[str]:
        """Get hash field value."""
        try:
            hash_data = await self.get(key)
            if hash_data and isinstance(hash_data, dict):
                return hash_data.get(field)
            return None
        except Exception as e:
            logger.error(f"Failed to hget {field} from {key} in local storage: {e}")
            return None
    
    async def hgetall(self, key: str) -> Optional[Dict[str, Any]]:
        """Get all hash fields."""
        try:
            hash_data = await self.get(key)
            if hash_data and isinstance(hash_data, dict):
                return hash_data
            return {}
        except Exception as e:
            logger.error(f"Failed to hgetall from {key} in local storage: {e}")
            return {}
    
    async def delete(self, key: str) -> int:
        """Delete a key."""
        try:
            file_path = self._get_file_path(key)
            if file_path.exists():
                file_path.unlink()
                return 1
            return 0
        except Exception as e:
            logger.error(f"Failed to delete key {key} from local storage: {e}")
            return 0
    
    async def incr(self, key: str) -> int:
        """Increment a key's value."""
        try:
            current = await self.get(key)
            value = int(current) if current else 0
            value += 1
            await self.set(key, str(value))
            return value
        except Exception as e:
            logger.error(f"Failed to increment key {key} in local storage: {e}")
            return 0
    
    async def lpush(self, key: str, *values) -> int:
        """Push values to the left of a list."""
        try:
            current = await self.get(key)
            if current and isinstance(current, list):
                current = list(values) + current
            else:
                current = list(values)
            await self.set(key, current)
            return len(current)
        except Exception as e:
            logger.error(f"Failed to lpush to key {key} in local storage: {e}")
            return 0
    
    async def lrange(self, key: str, start: int, end: int) -> List[str]:
        """Get a range of elements from a list."""
        try:
            current = await self.get(key)
            if current and isinstance(current, list):
                if end == -1:
                    return current[start:]
                return current[start:end+1]
            return []
        except Exception as e:
            logger.error(f"Failed to lrange from key {key} in local storage: {e}")
            return []
    
    async def ping(self) -> bool:
        """Test connection."""
        return True
    
    async def close(self):
        """Close connection (no-op for local storage)."""
        pass

class DatabaseClient:
    """Database client with Redis primary and local storage fallback."""
    
    def __init__(self):
        self._redis: Optional[Redis] = None
        self._fallback: Optional[LocalStorageFallback] = None
        self._using_fallback = False
    
    async def initialize(self) -> bool:
        """Initialize database connection."""
        try:
            # Try Redis first
            self._redis = Redis.from_url(config.redis_url, decode_responses=True)
            await self._redis.ping()
            logger.info("Connected to Redis successfully")
            self._using_fallback = False
            return True
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}")
            logger.info("Falling back to local storage")
            self._fallback = LocalStorageFallback()
            self._using_fallback = True
            return True
    
    def _get_client(self):
        """Get the active database client."""
        if self._using_fallback:
            return self._fallback
        return self._redis
    
    async def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """Set a key-value pair."""
        try:
            client = self._get_client()
            if self._using_fallback:
                return await client.set(key, value, ex)
            else:
                result = await client.set(key, value, ex=ex)
                return result is True
        except Exception as e:
            logger.error(f"Database set operation failed for key {key}: {e}")
            return False
    
    async def get(self, key: str) -> Optional[str]:
        """Get value by key."""
        try:
            client = self._get_client()
            return await client.get(key)
        except Exception as e:
            logger.error(f"Database get operation failed for key {key}: {e}")
            return None
    
    async def hset(self, key: str, mapping: Dict[str, Any]) -> int:
        """Set hash fields."""
        try:
            client = self._get_client()
            return await client.hset(key, mapping=mapping)
        except Exception as e:
            logger.error(f"Database hset operation failed for key {key}: {e}")
            return 0
    
    async def hget(self, key: str, field: str) -> Optional[str]:
        """Get hash field value."""
        try:
            client = self._get_client()
            return await client.hget(key, field)
        except Exception as e:
            logger.error(f"Database hget operation failed for key {key}, field {field}: {e}")
            return None
    
    async def hgetall(self, key: str) -> Dict[str, Any]:
        """Get all hash fields."""
        try:
            client = self._get_client()
            result = await client.hgetall(key)
            return result or {}
        except Exception as e:
            logger.error(f"Database hgetall operation failed for key {key}: {e}")
            return {}
    
    async def delete(self, key: str) -> int:
        """Delete a key."""
        try:
            client = self._get_client()
            return await client.delete(key)
        except Exception as e:
            logger.error(f"Database delete operation failed for key {key}: {e}")
            return 0
    
    async def incr(self, key: str) -> int:
        """Increment a key's value."""
        try:
            client = self._get_client()
            return await client.incr(key)
        except Exception as e:
            logger.error(f"Database incr operation failed for key {key}: {e}")
            return 0
    
    async def lpush(self, key: str, *values) -> int:
        """Push values to the left of a list."""
        try:
            client = self._get_client()
            return await client.lpush(key, *values)
        except Exception as e:
            logger.error(f"Database lpush operation failed for key {key}: {e}")
            return 0
    
    async def lrange(self, key: str, start: int, end: int) -> List[str]:
        """Get a range of elements from a list."""
        try:
            client = self._get_client()
            return await client.lrange(key, start, end)
        except Exception as e:
            logger.error(f"Database lrange operation failed for key {key}: {e}")
            return []
    
    async def close(self):
        """Close database connection."""
        try:
            if self._redis:
                await self._redis.close()
            if self._fallback:
                await self._fallback.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")

# Global database client instance
_db_client: Optional[DatabaseClient] = None

async def init_database() -> DatabaseClient:
    """Initialize and return database client."""
    global _db_client
    if _db_client is None:
        _db_client = DatabaseClient()
        await _db_client.initialize()
    return _db_client

async def get_database() -> DatabaseClient:
    """Get database client instance."""
    global _db_client
    if _db_client is None:
        _db_client = await init_database()
    return _db_client

async def close_database():
    """Close database connection."""
    global _db_client
    if _db_client:
        await _db_client.close()
        _db_client = None
