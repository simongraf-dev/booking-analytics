"""
Logging configuration for booking analytics
Centralized logging setup for all modules
"""
import logging
import os
from datetime import datetime

def setup_logging(script_name="booking-analytics", log_level=logging.INFO):
    """
    Setup logging configuration
    
    Args:
        script_name: Name of the script/module (for log file naming)
        log_level: Logging level (INFO, DEBUG, ERROR)
    """
    
    # Log directory
    log_dir = "/var/log/booking-analytics"
    log_file = f"{log_dir}/{script_name}.log"
    
    # Ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)
    
    # Clear any existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)
    
    # Console handler (for manual runs)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        handlers=[file_handler, console_handler]
    )
    
    # Return configured logger
    logger = logging.getLogger(script_name)
    logger.info(f"🔧 Logging initialized for {script_name}")
    logger.info(f"📁 Log file: {log_file}")
    
    return logger

def log_sync_start(logger, sync_type, date_range=None):
    """Log sync start with standardized format"""
    logger.info("=" * 50)
    logger.info(f"🚀 Starting {sync_type} sync")
    if date_range:
        logger.info(f"📅 Date range: {date_range}")
    logger.info(f"⏰ Started at: {datetime.now()}")

def log_sync_end(logger, sync_type, stats=None):
    """Log sync completion with stats"""
    logger.info(f"✅ {sync_type} sync completed")
    if stats:
        for key, value in stats.items():
            logger.info(f"📊 {key}: {value}")
    logger.info(f"⏰ Completed at: {datetime.now()}")
    logger.info("=" * 50)

if __name__ == "__main__":
    # Test logging setup
    test_logger = setup_logging("test")
    test_logger.info("✅ Logging test successful")
    test_logger.warning("⚠️ This is a warning")
    test_logger.error("❌ This is an error")