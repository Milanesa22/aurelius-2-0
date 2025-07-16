from loguru import logger
import sys
from pathlib import Path

def setup_logging():
    """Configure logging for the AURELIUS system."""
    
    # Remove default handler
    logger.remove()
    
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Console handler with colored output
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO",
        colorize=True
    )
    
    # File handler for all logs
    logger.add(
        "logs/aurelius.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        rotation="10 MB",
        retention="30 days",
        compression="zip",
        backtrace=True,
        diagnose=True
    )
    
    # Separate file for errors only
    logger.add(
        "logs/aurelius_errors.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="ERROR",
        rotation="5 MB",
        retention="60 days",
        compression="zip",
        backtrace=True,
        diagnose=True
    )
    
    # Separate file for sales and business critical events
    logger.add(
        "logs/aurelius_sales.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="INFO",
        rotation="5 MB",
        retention="90 days",
        compression="zip",
        filter=lambda record: "SALES" in record["message"] or "PAYMENT" in record["message"]
    )
    
    logger.info("Logging system initialized successfully")

def get_logger(name: str = None):
    """Get a logger instance with optional name."""
    if name:
        return logger.bind(name=name)
    return logger

# Initialize logging when module is imported
setup_logging()
