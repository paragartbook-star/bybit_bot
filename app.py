from flask import Flask, request, jsonify
import ccxt
import json
import logging
from datetime import datetime

app = Flask(__name__)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Bybit API Configuration
try:
    exchange = ccxt.bybit({
        'apiKey': 'YOUR_BYBIT_API_KEY',      # Yahan apni API key daalo
        'secret': 'YOUR_BYBIT_SECRET_KEY',    # Yahan apni Secret key daalo
        'enableRateLimit': True,
        'options': {
            'defaultType': 'linear',  # USDT perpetual futures
        }
    })
    exchange.set_sandbox_mode(False)  # True = Testnet, False = Live
    logger.info("✅ Bybit connection successful")
except Exception as e:
    logger.error(f"❌ Bybit connection failed: {e}")

# Security key (TradingView alerts verify karne ke liye)
SECRET_KEY = "apna_secret_password_123"  # Koi bhi strong password daalo

# Health check endpoint
@app.route('/')
def home():
    return jsonify({
        'status': 'Bot is running! 🚀',
        'timestamp': datetime.now().isoformat(),
        'exchange': 'Bybit',
        'mode': 'Live' if not exchange.options.get('test', False) else 'Testnet'
    })

# Ping endpoint (Render sleep mode rokne ke liye)
@app.route('/ping')
def ping():
    return jsonify({'status': 'pong', 'time': datetime.now().isoformat()})

# Main webhook endpoint
@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        # TradingView se data receive karo
        data = request.get_json()
        logger.info(f"📩 Received webhook: {json.dumps(data, indent=2)}")
        
        # Security check (optional - tumhare Pine Script mein key bhejo)
        # if data.get('key') != SECRET_KEY:
        #     logger.warning("⚠️ Invalid security key")
        #     return jsonify({'error': 'Unauthorized'}), 401
        
        # Parse webhook data
        action = data.get('action')  # "BUY" ya "SELL"
        symbol = data.get('symbol', 'BTCUSDT')  # Default BTC
        price = float(data.get('price', 0))
        sl = float(data.get('sl', 0))
        tp = float(data.get('tp', 0))
        qty = float(data.get('qty', 0.001))
        
        logger.info(f"📊 Processing: {action} {symbol} @ {price}")
        
        # Order placement
        if action == "BUY":
            order = place_long_order(symbol, qty, sl, tp)
        elif action == "SELL":
            order = place_short_order(symbol, qty, sl, tp)
        else:
            return jsonify({'error': 'Invalid action'}), 400
        
        logger.info(f"✅ Order placed: {order['id']}")
        
        return jsonify({
            'success': True,
            'order_id': order['id'],
            'symbol': symbol,
            'action': action,
            'qty': qty,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Long order placement function
def place_long_order(symbol, qty, sl_price, tp_price):
    try:
        # Market buy order
        order = exchange.create_market_buy_order(
            symbol=symbol,
            amount=qty,
            params={
                'stopLoss': {'triggerPrice': sl_price} if sl_price > 0 else None,
                'takeProfit': {'triggerPrice': tp_price} if tp_price > 0 else None
            }
        )
        return order
    except Exception as e:
        logger.error(f"❌ Long order failed: {e}")
        raise

# Short order placement function
def place_short_order(symbol, qty, sl_price, tp_price):
    try:
        # Market sell order
        order = exchange.create_market_sell_order(
            symbol=symbol,
            amount=qty,
            params={
                'stopLoss': {'triggerPrice': sl_price} if sl_price > 0 else None,
                'takeProfit': {'triggerPrice': tp_price} if tp_price > 0 else None
            }
        )
        return order
    except Exception as e:
        logger.error(f"❌ Short order failed: {e}")
        raise

# Account balance check endpoint
@app.route('/balance')
def balance():
    try:
        balance = exchange.fetch_balance()
        return jsonify({
            'total_usdt': balance['total'].get('USDT', 0),
            'free_usdt': balance['free'].get('USDT', 0),
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Render automatically sets PORT environment variable
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
