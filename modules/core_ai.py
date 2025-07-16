import asyncio
import aiohttp
import json
import bleach
from typing import Optional, Dict, List, Any
from config import config
from logging_setup import get_logger

logger = get_logger("core_ai")

class OpenAIClient:
    """OpenAI GPT-4/GPT-4o client for all AI operations."""
    
    def __init__(self):
        self.api_url = "https://api.openai.com/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {config.openai_api_key}",
            "Content-Type": "application/json"
        }
        self.default_model = "gpt-4o"
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=60)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session
    
    async def close(self):
        """Close the aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    def _sanitize_input(self, text: str) -> str:
        """Sanitize input to prevent code injection and XSS."""
        if not text:
            return ""
        
        # Remove potentially dangerous content
        sanitized = bleach.clean(text, tags=[], attributes={}, strip=True)
        
        # Additional sanitization for code injection prevention
        dangerous_patterns = [
            "javascript:", "data:", "vbscript:", "onload=", "onerror=",
            "<script", "</script>", "eval(", "setTimeout(", "setInterval("
        ]
        
        for pattern in dangerous_patterns:
            sanitized = sanitized.replace(pattern.lower(), "")
            sanitized = sanitized.replace(pattern.upper(), "")
        
        return sanitized.strip()
    
    async def generate_content(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None
    ) -> Optional[str]:
        """Generate content using OpenAI API."""
        try:
            # Sanitize inputs
            prompt = self._sanitize_input(prompt)
            if system_prompt:
                system_prompt = self._sanitize_input(system_prompt)
            
            if not prompt:
                logger.error("Empty prompt provided to generate_content")
                return None
            
            # Prepare messages
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})
            
            # Prepare payload
            payload = {
                "model": model or self.default_model,
                "messages": messages,
                "temperature": temperature
            }
            
            if max_tokens:
                payload["max_tokens"] = max_tokens
            
            session = await self._get_session()
            
            async with session.post(self.api_url, json=payload, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                    
                    if content:
                        logger.info(f"Successfully generated content (length: {len(content)})")
                        return content.strip()
                    else:
                        logger.warning("Empty content received from OpenAI API")
                        return None
                
                elif response.status == 429:
                    logger.warning("Rate limit exceeded for OpenAI API")
                    error_data = await response.json()
                    logger.error(f"Rate limit details: {error_data}")
                    return None
                
                else:
                    error_text = await response.text()
                    logger.error(f"OpenAI API error: {response.status} - {error_text}")
                    return None
        
        except asyncio.TimeoutError:
            logger.error("Timeout while calling OpenAI API")
            return None
        except aiohttp.ClientError as e:
            logger.error(f"HTTP client error while calling OpenAI API: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse OpenAI API response: {e}")
            return None
        except Exception as e:
            logger.exception(f"Unexpected error in generate_content: {e}")
            return None
    
    async def generate_sales_copy(
        self,
        product_info: str,
        target_audience: str = "general",
        tone: str = "professional"
    ) -> Optional[str]:
        """Generate sales copy for products/services."""
        system_prompt = f"""You are an expert sales copywriter. Create compelling, persuasive sales copy that:
        - Focuses on benefits over features
        - Uses a {tone} tone
        - Targets {target_audience} audience
        - Includes a clear call-to-action
        - Is concise and engaging
        - Avoids spam-like language
        """
        
        prompt = f"Create sales copy for: {product_info}"
        
        return await self.generate_content(
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=0.8
        )
    
    async def generate_social_post(
        self,
        topic: str,
        platform: str,
        style: str = "engaging"
    ) -> Optional[str]:
        """Generate social media posts for different platforms."""
        platform_limits = {
            "twitter": 280,
            "mastodon": 500,
            "discord": 2000
        }
        
        char_limit = platform_limits.get(platform.lower(), 280)
        
        system_prompt = f"""You are a social media expert. Create {style} posts for {platform} that:
        - Stay under {char_limit} characters
        - Are engaging and authentic
        - Include relevant hashtags (2-3 max)
        - Encourage interaction
        - Avoid controversial topics
        - Match the platform's culture
        """
        
        prompt = f"Create a {platform} post about: {topic}"
        
        return await self.generate_content(
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=0.9,
            max_tokens=150
        )
    
    async def generate_reply(
        self,
        original_message: str,
        context: str = "",
        tone: str = "helpful"
    ) -> Optional[str]:
        """Generate replies to messages/comments."""
        system_prompt = f"""You are a helpful assistant responding to messages. Your replies should be:
        - {tone} in tone
        - Relevant to the original message
        - Concise and clear
        - Professional but friendly
        - Avoid controversial topics
        - Provide value when possible
        """
        
        prompt = f"Reply to this message: '{original_message}'"
        if context:
            prompt += f"\nContext: {context}"
        
        return await self.generate_content(
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=0.7,
            max_tokens=200
        )
    
    async def generate_follow_up(
        self,
        previous_interaction: str,
        customer_response: str,
        sales_stage: str = "initial"
    ) -> Optional[str]:
        """Generate follow-up messages for sales interactions."""
        system_prompt = f"""You are a sales professional creating follow-up messages. Your message should:
        - Be personalized based on the previous interaction
        - Match the {sales_stage} sales stage
        - Be helpful, not pushy
        - Include a soft call-to-action
        - Build rapport and trust
        - Address any concerns raised
        """
        
        prompt = f"""Previous interaction: {previous_interaction}
        Customer response: {customer_response}
        Generate an appropriate follow-up message."""
        
        return await self.generate_content(
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=0.6
        )
    
    async def analyze_sentiment(self, text: str) -> Optional[Dict[str, Any]]:
        """Analyze sentiment and extract insights from text."""
        system_prompt = """You are a sentiment analysis expert. Analyze the given text and return a JSON response with:
        - sentiment: positive/negative/neutral
        - confidence: 0.0-1.0
        - key_emotions: list of detected emotions
        - intent: purchase_intent/question/complaint/compliment/other
        - urgency: low/medium/high
        
        Return only valid JSON, no additional text."""
        
        prompt = f"Analyze this text: {text}"
        
        response = await self.generate_content(
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=0.3
        )
        
        if response:
            try:
                return json.loads(response)
            except json.JSONDecodeError:
                logger.error("Failed to parse sentiment analysis JSON response")
                return None
        
        return None
    
    async def optimize_content(
        self,
        content: str,
        optimization_goal: str = "engagement"
    ) -> Optional[str]:
        """Optimize existing content for better performance."""
        system_prompt = f"""You are a content optimization expert. Improve the given content to maximize {optimization_goal}:
        - Keep the core message intact
        - Improve clarity and readability
        - Enhance emotional appeal
        - Optimize for the specified goal
        - Maintain authenticity
        """
        
        prompt = f"Optimize this content: {content}"
        
        return await self.generate_content(
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=0.5
        )

# Global AI client instance
_ai_client: Optional[OpenAIClient] = None

async def get_ai_client() -> OpenAIClient:
    """Get AI client instance."""
    global _ai_client
    if _ai_client is None:
        _ai_client = OpenAIClient()
    return _ai_client

async def close_ai_client():
    """Close AI client."""
    global _ai_client
    if _ai_client:
        await _ai_client.close()
        _ai_client = None

# Convenience functions
async def generate_content(prompt: str, **kwargs) -> Optional[str]:
    """Generate content using the global AI client."""
    client = await get_ai_client()
    return await client.generate_content(prompt, **kwargs)

async def generate_sales_copy(product_info: str, **kwargs) -> Optional[str]:
    """Generate sales copy using the global AI client."""
    client = await get_ai_client()
    return await client.generate_sales_copy(product_info, **kwargs)

async def generate_social_post(topic: str, platform: str, **kwargs) -> Optional[str]:
    """Generate social media post using the global AI client."""
    client = await get_ai_client()
    return await client.generate_social_post(topic, platform, **kwargs)

async def generate_reply(original_message: str, **kwargs) -> Optional[str]:
    """Generate reply using the global AI client."""
    client = await get_ai_client()
    return await client.generate_reply(original_message, **kwargs)

async def generate_follow_up(previous_interaction: str, customer_response: str, **kwargs) -> Optional[str]:
    """Generate follow-up message using the global AI client."""
    client = await get_ai_client()
    return await client.generate_follow_up(previous_interaction, customer_response, **kwargs)

async def analyze_sentiment(text: str) -> Optional[Dict[str, Any]]:
    """Analyze sentiment using the global AI client."""
    client = await get_ai_client()
    return await client.analyze_sentiment(text)

async def optimize_content(content: str, **kwargs) -> Optional[str]:
    """Optimize content using the global AI client."""
    client = await get_ai_client()
    return await client.optimize_content(content, **kwargs)
