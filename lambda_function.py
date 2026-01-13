import json
import os
import logging
import time
from typing import Dict, Optional, Any
from pybit.unified_trading import HTTP
import requests

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
BYBIT_API_KEY = os.environ.get('BYBIT_API_KEY')
BYBIT_API_SECRET = os.environ.get('BYBIT_API_SECRET')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
BYBIT_TESTNET = os.environ.get('BYBIT_TESTNET', 'false').lower() == 'true'


def validate_environment_variables() -> bool:
    """Validate that all required environment variables are set."""
    required_vars = {
        'BYBIT_API_KEY': BYBIT_API_KEY,
        'BYBIT_API_SECRET': BYBIT_API_SECRET,
        'TELEGRAM_BOT_TOKEN': TELEGRAM_BOT_TOKEN,
        'TELEGRAM_CHAT_ID': TELEGRAM_CHAT_ID
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    
    return True


def send_telegram_message(message: str) -> bool:
    """
    Send a message to Telegram bot.
    
    Args:
        message: Message text to send
        
    Returns:
        True if successful, False otherwise
    """
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message,
            'parse_mode': 'HTML'
        }
        
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        
        logger.info("Telegram message sent successfully")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send Telegram message: {str(e)}")
        return False


def format_telegram_notification(
    symbol: str,
    action: str,
    entry_price: float,
    quantity: float,
    stop_loss: Optional[float],
    take_profit: Optional[float],
    order_id: Optional[str] = None
) -> str:
    """
    Format trade notification for Telegram.
    
    Args:
        symbol: Trading symbol
        action: Order action (Buy/Sell)
        entry_price: Entry price
        quantity: Position quantity
        stop_loss: Stop loss price (optional)
        take_profit: Take profit price (optional)
        order_id: Bybit order ID (optional)
        
    Returns:
        Formatted message string
    """
    emoji = "🟢" if action.upper() in ["BUY", "LONG"] else "🔴"
    
    message = f"{emoji} <b>Trade Executed</b>\n\n"
    message += f"<b>Symbol:</b> {symbol}\n"
    message += f"<b>Action:</b> {action.upper()}\n"
    message += f"<b>Quantity:</b> {quantity}\n"
    message += f"<b>Entry:</b> ${entry_price:.4f}\n"
    
    if stop_loss:
        message += f"<b>Stop Loss:</b> ${stop_loss:.4f}\n"
    
    if take_profit:
        message += f"<b>Take Profit:</b> ${take_profit:.4f}\n"
    
    if order_id:
        message += f"<b>Order ID:</b> {order_id}\n"
    
    return message


def calculate_position_size(
    bybit_client: HTTP,
    symbol: str,
    price: float,
    risk_percent: float = 0.02
) -> Optional[float]:
    """
    Calculate dynamic position size based on account balance.
    
    Args:
        bybit_client: Bybit HTTP client instance
        symbol: Trading symbol
        price: Entry price
        risk_percent: Percentage of account balance to risk (default: 2%)
        
    Returns:
        Position size (quantity) or None if calculation fails
    """
    try:
        # Get account balance
        response = bybit_client.get_wallet_balance(
            accountType="UNIFIED",
            coin="USDT"
        )
        
        if response.get('retCode') != 0:
            logger.error(f"Failed to get account balance: {response.get('retMsg')}")
            return None
        
        # Extract available balance
        result = response.get('result', {})
        list_data = result.get('list', [])
        
        if not list_data:
            logger.error("No account data returned")
            return None
        
        # Find USDT balance
        total_equity = 0.0
        for account in list_data:
            for coin in account.get('coin', []):
                if coin.get('coin') == 'USDT':
                    total_equity = float(coin.get('walletBalance', 0))
                    break
            if total_equity > 0:
                break
        
        if total_equity <= 0:
            logger.error("No USDT balance found")
            return None
        
        # Calculate position size based on risk percentage
        risk_amount = total_equity * risk_percent
        position_size = risk_amount / price
        
        # Get symbol info to apply minimum/maximum constraints
        try:
            instruments_response = bybit_client.get_instruments_info(
                category="linear",
                symbol=symbol
            )
            
            if instruments_response.get('retCode') == 0:
                result = instruments_response.get('result', {})
                instruments = result.get('list', [])
                
                if instruments:
                    instrument = instruments[0]
                    min_qty = float(instrument.get('lotSizeFilter', {}).get('minQty', 0))
                    max_qty = float(instrument.get('lotSizeFilter', {}).get('maxQty', float('inf')))
                    qty_step = float(instrument.get('lotSizeFilter', {}).get('qtyStep', 0.001))
                    
                    # Round to nearest qty step
                    position_size = round(position_size / qty_step) * qty_step
                    
                    # Apply min/max constraints
                    position_size = max(min_qty, min(position_size, max_qty))
                    
                    logger.info(f"Calculated position size: {position_size} for symbol {symbol}")
                    return position_size
                    
        except Exception as e:
            logger.warning(f"Could not fetch symbol info, using unrounded position size: {str(e)}")
        
        logger.info(f"Calculated position size: {position_size} for symbol {symbol}")
        return position_size
        
    except Exception as e:
        logger.error(f"Error calculating position size: {str(e)}")
        return None


def execute_bybit_order(
    bybit_client: HTTP,
    action: str,
    symbol: str,
    quantity: float,
    stop_loss: Optional[float] = None,
    take_profit: Optional[float] = None
) -> Dict[str, Any]:
    """
    Execute order on Bybit Perpetual Futures.
    
    Args:
        bybit_client: Bybit HTTP client instance
        action: Order action (Buy/Long or Sell/Short)
        symbol: Trading symbol
        quantity: Position quantity
        stop_loss: Stop loss price (optional)
        take_profit: Take profit price (optional)
        
    Returns:
        Dictionary with order result or error information
    """
    try:
        # Normalize action to Bybit side
        side = "Buy" if action.upper() in ["BUY", "LONG"] else "Sell"
        
        # Place market order
        order_response = bybit_client.place_order(
            category="linear",
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=str(quantity),
            positionIdx=0,  # One-Way Mode
            reduceOnly=False
        )
        
        if order_response.get('retCode') != 0:
            error_msg = order_response.get('retMsg', 'Unknown error')
            logger.error(f"Bybit order failed: {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'response': order_response
            }
        
        order_id = order_response.get('result', {}).get('orderId')
        order_link_id = order_response.get('result', {}).get('orderLinkId')
        
        logger.info(f"Order placed successfully: {order_id}")
        
        # Set stop loss and take profit using set_trading_stop (recommended method)
        # Wait a moment for the position to be established
        time.sleep(0.5)  # Brief delay to ensure position is created
        
        if stop_loss or take_profit:
            try:
                trading_stop_params = {
                    "category": "linear",
                    "symbol": symbol,
                    "positionIdx": 0
                }
                
                if stop_loss:
                    trading_stop_params["stopLoss"] = str(stop_loss)
                
                if take_profit:
                    trading_stop_params["takeProfit"] = str(take_profit)
                
                sl_tp_response = bybit_client.set_trading_stop(**trading_stop_params)
                
                if sl_tp_response.get('retCode') == 0:
                    logger.info(f"Stop loss and take profit set successfully")
                else:
                    logger.warning(f"Failed to set stop loss/take profit: {sl_tp_response.get('retMsg')}")
                    
            except Exception as e:
                logger.warning(f"Error setting stop loss/take profit: {str(e)}")
        
        # Get fill price from order execution
        fill_price = None
        try:
            execution_response = bybit_client.get_open_orders(
                category="linear",
                symbol=symbol
            )
            # Try to get from trade history instead
            trade_response = bybit_client.get_executions(
                category="linear",
                symbol=symbol,
                limit=1
            )
            
            if trade_response.get('retCode') == 0:
                executions = trade_response.get('result', {}).get('list', [])
                if executions:
                    fill_price = float(executions[0].get('execPrice', 0))
                    
        except Exception as e:
            logger.warning(f"Could not retrieve fill price: {str(e)}")
        
        return {
            'success': True,
            'order_id': order_id,
            'order_link_id': order_link_id,
            'fill_price': fill_price
        }
        
    except Exception as e:
        logger.error(f"Error executing Bybit order: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler function for TradingView webhook.
    
    Expected JSON payload:
    {
        "action": "BUY" or "SELL",
        "symbol": "BTCUSDT",
        "qty": 0.01 (optional),
        "price": 50000.0 (optional, for position sizing),
        "SL": 49000.0 (optional),
        "TP": 51000.0 (optional)
    }
    
    Args:
        event: Lambda event containing TradingView alert data
        context: Lambda context object
        
    Returns:
        HTTP response dictionary
    """
    # Validate environment variables
    if not validate_environment_variables():
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': 'Missing required environment variables'
            })
        }
    
    try:
        # Parse request body
        if isinstance(event.get('body'), str):
            body = json.loads(event['body'])
        else:
            body = event.get('body', {})
        
        # If body is None or empty, try to get from event directly
        if not body:
            body = event
        
        logger.info(f"Received webhook payload: {json.dumps(body)}")
        
        # Extract parameters
        action = body.get('action') or body.get('Action') or body.get('ACTION')
        symbol = body.get('symbol') or body.get('Symbol') or body.get('SYMBOL')
        qty = body.get('qty') or body.get('Qty') or body.get('QTY') or body.get('quantity') or body.get('Quantity')
        price = body.get('price') or body.get('Price') or body.get('PRICE') or body.get('entry') or body.get('Entry')
        stop_loss = body.get('SL') or body.get('sl') or body.get('stopLoss') or body.get('StopLoss')
        take_profit = body.get('TP') or body.get('tp') or body.get('takeProfit') or body.get('TakeProfit')
        
        # Validate required fields
        if not action:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'success': False,
                    'error': 'Missing required field: action'
                })
            }
        
        if not symbol:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'success': False,
                    'error': 'Missing required field: symbol'
                })
            }
        
        # Normalize symbol format (Bybit uses uppercase)
        symbol = symbol.upper()
        symbol = symbol.replace('.P', '')
        
        # Initialize Bybit client
        try:
            bybit_client = HTTP(
                testnet=BYBIT_TESTNET,
                api_key=BYBIT_API_KEY,
                api_secret=BYBIT_API_SECRET
            )
        except Exception as e:
            logger.error(f"Failed to initialize Bybit client: {str(e)}")
            send_telegram_message(f"❌ <b>Error:</b> Failed to initialize Bybit client: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'success': False,
                    'error': f'Failed to initialize Bybit client: {str(e)}'
                })
            }
        
        # Calculate quantity if not provided
        quantity = None
        if qty:
            try:
                quantity = float(qty)
            except (ValueError, TypeError):
                logger.warning(f"Invalid qty value: {qty}, will calculate dynamically")
        
        if not quantity or quantity <= 0:
            if not price:
                logger.error("Cannot calculate position size: price is required when qty is not provided")
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'success': False,
                        'error': 'Either qty or price must be provided for position sizing'
                    })
                }
            
            try:
                entry_price = float(price)
                quantity = calculate_position_size(bybit_client, symbol, entry_price)
                
                if not quantity or quantity <= 0:
                    return {
                        'statusCode': 500,
                        'body': json.dumps({
                            'success': False,
                            'error': 'Failed to calculate position size'
                        })
                    }
                    
            except (ValueError, TypeError) as e:
                logger.error(f"Invalid price value: {price}")
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'success': False,
                        'error': f'Invalid price value: {price}'
                    })
                }
        
        # Convert optional fields to float
        try:
            sl_price = float(stop_loss) if stop_loss else None
        except (ValueError, TypeError):
            logger.warning(f"Invalid stop loss value: {stop_loss}")
            sl_price = None
        
        try:
            tp_price = float(take_profit) if take_profit else None
        except (ValueError, TypeError):
            logger.warning(f"Invalid take profit value: {take_profit}")
            tp_price = None
        
        # Execute order
        order_result = execute_bybit_order(
            bybit_client=bybit_client,
            action=action,
            symbol=symbol,
            quantity=quantity,
            stop_loss=sl_price,
            take_profit=tp_price
        )
        
        if not order_result.get('success'):
            error_msg = order_result.get('error', 'Unknown error')
            send_telegram_message(f"❌ <b>Order Failed</b>\n\nSymbol: {symbol}\nAction: {action}\nError: {error_msg}")
            
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'success': False,
                    'error': error_msg
                })
            }
        
        # Get fill price (use provided price as fallback)
        fill_price = order_result.get('fill_price')
        if not fill_price and price:
            try:
                fill_price = float(price)
            except (ValueError, TypeError):
                pass
        
        # Send Telegram notification
        telegram_message = format_telegram_notification(
            symbol=symbol,
            action=action,
            entry_price=fill_price or 0.0,
            quantity=quantity,
            stop_loss=sl_price,
            take_profit=tp_price,
            order_id=order_result.get('order_id')
        )
        send_telegram_message(telegram_message)
        
        # Return success response
        response_data = {
            'success': True,
            'symbol': symbol,
            'action': action,
            'quantity': quantity,
            'order_id': order_result.get('order_id'),
            'stop_loss': sl_price,
            'take_profit': tp_price
        }
        
        logger.info(f"Order executed successfully: {response_data}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(response_data)
        }
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {str(e)}")
        return {
            'statusCode': 400,
            'body': json.dumps({
                'success': False,
                'error': f'Invalid JSON: {str(e)}'
            })
        }
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        send_telegram_message(f"❌ <b>Unexpected Error</b>\n\n{str(e)}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': f'Internal server error: {str(e)}'
            })
        }

