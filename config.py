import os

class Config:
    # Bybit API Credentials (Environment variables se load karenge)
    BYBIT_API_KEY = os.environ.get('BYBIT_API_KEY', 'your_api_key_here')
    BYBIT_SECRET_KEY = os.environ.get('BYBIT_SECRET_KEY', 'your_secret_key_here')
    
    # Security
    SECRET_KEY = os.environ.get('SECRET_KEY', 'apna_secret_password_123')
    
    # Trading Settings
    DEFAULT_SYMBOL = 'BTCUSDT'
    DEFAULT_QTY = 0.001
    
    # Mode (True = Testnet, False = Live)
    TESTNET_MODE = os.environ.get('TESTNET_MODE', 'False').lower() == 'true'
    
    # Risk Management
    MAX_DAILY_LOSS_PERCENT = 2.0
    RISK_PER_TRADE_PERCENT = 1.0
