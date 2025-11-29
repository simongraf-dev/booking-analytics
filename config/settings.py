"""
Configuration loader for booking analytics project
Loads environment variables and provides config objects
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'), 
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# API Configuration  
API_CONFIG = {
    'url': os.getenv('GRAPHQL_API_URL'),
    'token': os.getenv('ACCOUNT_TOKEN'),
    'location_id': os.getenv('LOCATION_ID')
}

# Date Configuration
DATE_CONFIG = {
    'default_start': os.getenv('DEFAULT_START_DATE'),
    'default_end': os.getenv('DEFAULT_END_DATE')
}

# Validate required settings
def validate_config():
    """Check if all required environment variables are set"""
    required = [
        'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD',
        'GRAPHQL_API_URL', 'ACCOUNT_TOKEN', 'LOCATION_ID'
    ]
    
    missing = [var for var in required if not os.getenv(var)]
    
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")
    
    print("✅ All config variables loaded successfully")

if __name__ == "__main__":
    validate_config()

