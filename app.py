from flask import Flask, request, jsonify
import ccxt
import json
import logging
from datetime import datetime
import os
import requests

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
TESTNET_MODE = os.environ.get('TESTNET_MODE', 'True').lower() == 'true'

try:
    exchange = ccxt.bybit({
        'apiKey': os.environ.get('BYBIT_API_KEY', ''),
        'secret': os.environ.get('BYBIT_SECRET_KEY', ''),
        'enableRateLimit': True,
        'timeout': 30000,
        'options': {
            'defaultType': 'linear',
            'adjustForTimeDifference': True
        },
        'headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Content-Type': 'application/json'
        }
    })
    
    exchange.set_sandbox_mode(TESTNET_MODE)
    
    mode_text = "Testnet" if TESTNET_MODE else "Live"
    logger.info(f"✅ Bybit connection initialized - Mode: {mode_text}")
except Exception as e:
    logger.error(f"❌ Bybit initialization failed: {e}")

@app.route('/')
def home():
    mode_text = "Testnet" if TESTNET_MODE else "Live"
    return jsonify({
        'status': 'Bot is running! 🚀',
        'timestamp': datetime.now().isoformat(),
        'exchange': 'Bybit',
        'mode': mode_text
    })

@app.route('/ping')
def ping():
    return jsonify({'status': 'pong', 'time': datetime.now().isoformat()})

@app.route('/balance')
def balance():
    try:
        balance = exchange.fetch_balance()
        return jsonify({
            'total_usdt': balance['total'].get('USDT', 0),
            'free_usdt': balance['free'].get('USDT', 0),
            'used_usdt': balance['used'].get('USDT', 0),
            'timestamp': datetime.now().isoformat()
        })
    except ccxt.AuthenticationError as e:
        logger.error(f"❌ Authentication failed: {e}")
        return jsonify({'error': 'Invalid API keys', 'details': str(e)}), 401
    except ccxt.NetworkError as e:
        logger.error(f"❌ Network error: {e}")
        return jsonify({'error': 'Network connection failed', 'details': str(e)}), 503
    except Exception as e:
        logger.error(f"❌ Balance fetch error: {e}")
        return jsonify({'error': str(e), 'type': type(e).__name__}), 500

@app.route('/test-connection')
def test_connection():
    try:
        # Server time fetch
        server_time = exchange.fetch_time()
        
        return jsonify({
            'status': 'success',
            'server_time': datetime.fromtimestamp(server_time/1000).isoformat(),
            'connection': 'OK',
            'mode': "Testnet" if TESTNET_MODE else "Live"
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'error_type': type(e).__name__
        }), 500

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.get_json()
        logger.info(f"📩 Received webhook: {json.dumps(data, indent=2)}")
        
        action = data.get('action')
        symbol = data.get('symbol', 'BTCUSDT')
        price = float(data.get('price', 0))
        sl = float(data.get('sl', 0))
        tp = float(data.get('tp', 0))
        qty = float(data.get('qty', 0.001))
        
        logger.info(f"📊 Processing: {action} {symbol} @ {price}")
        
        if action == "BUY":
            order = place_long_order(symbol, qty, sl, tp)
        elif action == "SELL":
            order = place_short_order(symbol, qty, sl, tp)
        else:
            return jsonify({'error': 'Invalid action'}), 400
        
        logger.info(f"✅ Order placed: {order.get('id', 'N/A')}")
        
        return jsonify({
            'success': True,
            'order_id': order.get('id'),
            'symbol': symbol,
            'action': action,
            'qty': qty,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"❌ Webhook error: {str(e)}")
        return jsonify({'error': str(e), 'type': type(e).__name__}), 500

def place_long_order(symbol, qty, sl_price, tp_price):
    try:
        order = exchange.create_market_buy_order(symbol, qty)
        logger.info(f"✅ Long order created: {order['id']}")
        
        # Set SL/TP if provided
        if sl_price > 0 or tp_price > 0:
            try:
                exchange.set_leverage(10, symbol)  # 10x leverage
            except:
                pass
        
        return order
    except Exception as e:
        logger.error(f"❌ Long order failed: {e}")
        raise

def place_short_order(symbol, qty, sl_price, tp_price):
    try:
        order = exchange.create_market_sell_order(symbol, qty)
        logger.info(f"✅ Short order created: {order['id']}")
        
        return order
    except Exception as e:
        logger.error(f"❌ Short order failed: {e}")
        raise

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
