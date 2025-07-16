import os
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError, validator
from typing import Optional
from loguru import logger

# Load environment variables
load_dotenv()

class Config(BaseModel):
    # OpenAI Configuration
    openai_api_key: str = Field(..., env="OPENAI_API_KEY")
    
    # Twitter/X Configuration
    twitter_api_key: str = Field(..., env="TWITTER_API_KEY")
    twitter_api_secret: str = Field(..., env="TWITTER_API_SECRET")
    twitter_bearer_token: str = Field(..., env="TWITTER_BEARER_TOKEN")
    
    # Discord Configuration
    discord_token: str = Field(..., env="DISCORD_TOKEN")
    discord_webhook_url: str = Field(..., env="DISCORD_WEBHOOK_URL")
    
    # Mastodon Configuration
    mastodon_token: str = Field(..., env="MASTODON_TOKEN")
    mastodon_instance_url: str = Field(..., env="MASTODON_INSTANCE_URL")
    
    # PayPal Configuration
    paypal_client_id: str = Field(..., env="PAYPAL_CLIENT_ID")
    paypal_client_secret: str = Field(..., env="PAYPAL_CLIENT_SECRET")
    paypal_environment: str = Field("sandbox", env="PAYPAL_ENVIRONMENT")
    
    # Redis Configuration
    redis_url: str = Field("redis://localhost:6379/0", env="REDIS_URL")
    
    # Rate Limits
    twitter_rate_limit: int = Field(100, env="TWITTER_RATE_LIMIT")
    mastodon_rate_limit: int = Field(100, env="MASTODON_RATE_LIMIT")
    discord_rate_limit: int = Field(100, env="DISCORD_RATE_LIMIT")
    
    # Scheduling intervals (in minutes)
    post_interval: int = Field(60, env="POST_INTERVAL")
    analytics_interval: int = Field(1440, env="ANALYTICS_INTERVAL")
    learning_interval: int = Field(720, env="LEARNING_INTERVAL")
    
    @validator('openai_api_key')
    def validate_openai_key(cls, v):
        if not v or v == "your_openai_api_key_here":
            raise ValueError("OpenAI API key is required and must be set to a valid key")
        return v
    
    @validator('twitter_bearer_token')
    def validate_twitter_token(cls, v):
        if not v or v == "your_twitter_bearer_token_here":
            raise ValueError("Twitter Bearer Token is required and must be set to a valid token")
        return v
    
    @validator('discord_token')
    def validate_discord_token(cls, v):
        if not v or v == "your_discord_token_here":
            raise ValueError("Discord Token is required and must be set to a valid token")
        return v
    
    @validator('mastodon_token')
    def validate_mastodon_token(cls, v):
        if not v or v == "your_mastodon_token_here":
            raise ValueError("Mastodon Token is required and must be set to a valid token")
        return v
    
    @validator('paypal_client_id')
    def validate_paypal_client_id(cls, v):
        if not v or v == "your_paypal_client_id_here":
            raise ValueError("PayPal Client ID is required and must be set to a valid ID")
        return v
    
    @validator('paypal_environment')
    def validate_paypal_environment(cls, v):
        if v not in ['sandbox', 'live']:
            raise ValueError("PayPal environment must be either 'sandbox' or 'live'")
        return v
    
    class Config:
        env_file = '.env'
        case_sensitive = False

def load_config() -> Config:
    """Load and validate configuration with detailed error messages."""
    try:
        # Get values from environment
        config_data = {
            'openai_api_key': os.getenv('OPENAI_API_KEY', ''),
            'twitter_api_key': os.getenv('TWITTER_API_KEY', ''),
            'twitter_api_secret': os.getenv('TWITTER_API_SECRET', ''),
            'twitter_bearer_token': os.getenv('TWITTER_BEARER_TOKEN', ''),
            'discord_token': os.getenv('DISCORD_TOKEN', ''),
            'discord_webhook_url': os.getenv('DISCORD_WEBHOOK_URL', ''),
            'mastodon_token': os.getenv('MASTODON_TOKEN', ''),
            'mastodon_instance_url': os.getenv('MASTODON_INSTANCE_URL', 'https://mastodon.social'),
            'paypal_client_id': os.getenv('PAYPAL_CLIENT_ID', ''),
            'paypal_client_secret': os.getenv('PAYPAL_CLIENT_SECRET', ''),
            'paypal_environment': os.getenv('PAYPAL_ENVIRONMENT', 'sandbox'),
            'redis_url': os.getenv('REDIS_URL', 'redis://localhost:6379/0'),
            'twitter_rate_limit': int(os.getenv('TWITTER_RATE_LIMIT', '100')),
            'mastodon_rate_limit': int(os.getenv('MASTODON_RATE_LIMIT', '100')),
            'discord_rate_limit': int(os.getenv('DISCORD_RATE_LIMIT', '100')),
            'post_interval': int(os.getenv('POST_INTERVAL', '60')),
            'analytics_interval': int(os.getenv('ANALYTICS_INTERVAL', '1440')),
            'learning_interval': int(os.getenv('LEARNING_INTERVAL', '720')),
        }
        
        config = Config(**config_data)
        logger.info("Configuration loaded and validated successfully")
        return config
        
    except ValidationError as e:
        error_msg = "Configuration validation failed:\n"
        for error in e.errors():
            field = error['loc'][0] if error['loc'] else 'unknown'
            message = error['msg']
            error_msg += f"  - {field}: {message}\n"
        
        error_msg += "\nPlease check your .env file and ensure all required API keys are set correctly."
        logger.error(error_msg)
        raise SystemExit(error_msg)
    
    except Exception as e:
        error_msg = f"Failed to load configuration: {str(e)}"
        logger.error(error_msg)
        raise SystemExit(error_msg)

# Initialize configuration
try:
    config = load_config()
except SystemExit:
    raise
except Exception as e:
    logger.error(f"Critical error during configuration initialization: {e}")
    raise
