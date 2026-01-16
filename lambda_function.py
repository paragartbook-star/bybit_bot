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


def send_telegram_message(message: str, parse_mode: str = 'HTML') -> bool:
    """
    Send a message to Telegram bot.
    
    Args:
        message: Message text to send
        parse_mode: Telegram parse mode (default: HTML)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message,
            'parse_mode': parse_mode
        }
        
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        
        logger.info("Telegram message sent successfully")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send Telegram message: {str(e)}")
        return False


def send_telegram_error(error_title: str, error_details: Dict[str, Any]) -> None:
    """
    Send formatted error message to Telegram.
    
    Args:
        error_title: Error title/category
        error_details: Dictionary containing error details
    """
    message = f"❌ <b>{error_title}</b>\n\n"
    
    for key, value in error_details.items():
        message += f"<b>{key}:</b> {value}\n"
    
    send_telegram_message(message)


def format_telegram_notification(
    symbol: str,
    action: str,
    entry_price: float,
    quantity: float,
    stop_loss: Optional[float],
    take_profit: Optional[float],
    order_id: Optional[str] = None,
    trailing_stop: Optional[float] = None
) -> str:
    """
    Format trade notification for Telegram.
    
    Args:
        symbol: Trading symbol
        action: Order action (Buy/Sell/Update)
        entry_price: Entry price
        quantity: Position quantity
        stop_loss: Stop loss price (optional)
        take_profit: Take profit price (optional)
        order_id: Bybit order ID (optional)
        trailing_stop: Trailing stop value (optional)
        
    Returns:
        Formatted message string
    """
    if action.upper() == "UPDATE":
        emoji = "🔄"
    else:
        emoji = "🟢" if action.upper() in ["BUY", "LONG"] else "🔴"
    
    message = f"{emoji} <b>Trade {action.upper()}</b>\n\n"
    message += f"<b>Symbol:</b> {symbol}\n"
    
    if action.upper() != "UPDATE":
        message += f"<b>Action:</b> {action.upper()}\n"
        message += f"<b>Quantity:</b> {quantity}\n"
        message += f"<b>Entry:</b> ${entry_price:.4f}\n"
    
    if stop_loss:
        message += f"<b>Stop Loss:</b> ${stop_loss:.4f}\n"
    
    if take_profit:
        message += f"<b>Take Profit:</b> ${take_profit:.4f}\n"
    
    if trailing_stop:
        message += f"<b>Trailing Stop:</b> ${trailing_stop:.4f}\n"
    
    if order_id:
        message += f"<b>Order ID:</b> {order_id}\n"
    
    message += f"\n<i>Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}</i>"
    
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
        # ✅ Bybit V5: Get account balance
        response = bybit_client.get_wallet_balance(
            accountType="UNIFIED",
            coin="USDT"
        )
        
        # ✅ Check Bybit V5 response
        if response.get('retCode') != 0:
            error_msg = response.get('retMsg', 'Unknown error')
            logger.error(f"Failed to get account balance - retCode: {response.get('retCode')}, retMsg: {error_msg}")
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
        
        logger.info(f"Account USDT Balance: {total_equity}")
        
        # Calculate position size based on risk percentage
        risk_amount = total_equity * risk_percent
        position_size = risk_amount / price
        
        # ✅ Bybit V5: Get symbol info with category parameter
        try:
            instruments_response = bybit_client.get_instruments_info(
                category="linear",
                symbol=symbol
            )
            
            # ✅ Check Bybit V5 response
            if instruments_response.get('retCode') == 0:
                result = instruments_response.get('result', {})
                instruments = result.get('list', [])
                
                if instruments:
                    instrument = instruments[0]
                    lot_size_filter = instrument.get('lotSizeFilter', {})
                    
                    min_qty = float(lot_size_filter.get('minOrderQty', 0))
                    max_qty = float(lot_size_filter.get('maxOrderQty', float('inf')))
                    qty_step = float(lot_size_filter.get('qtyStep', 0.001))
                    
                    # Round to nearest qty step
                    position_size = round(position_size / qty_step) * qty_step
                    
                    # Apply min/max constraints
                    position_size = max(min_qty, min(position_size, max_qty))
                    
                    logger.info(f"Calculated position size: {position_size} for symbol {symbol}")
                    logger.info(f"Symbol constraints - Min: {min_qty}, Max: {max_qty}, Step: {qty_step}")
                    
                    return position_size
            else:
                logger.warning(f"Could not fetch symbol info - retCode: {instruments_response.get('retCode')}, retMsg: {instruments_response.get('retMsg')}")
                    
        except Exception as e:
            logger.warning(f"Could not fetch symbol info, using unrounded position size: {str(e)}")
        
        logger.info(f"Calculated position size: {position_size} for symbol {symbol}")
        return position_size
        
    except Exception as e:
        logger.error(f"Error calculating position size: {str(e)}")
        return None


def update_position_stops(
    bybit_client: HTTP,
    symbol: str,
    stop_loss: Optional[float] = None,
    take_profit: Optional[float] = None,
    trailing_stop: Optional[float] = None
) -> Dict[str, Any]:
    """
    Update stop loss, take profit, or trailing stop for an existing position.
    
    Args:
        bybit_client: Bybit HTTP client instance
        symbol: Trading symbol
        stop_loss: Stop loss price (optional)
        take_profit: Take profit price (optional)
        trailing_stop: Trailing stop value in price units (optional)
        
    Returns:
        Dictionary with update result or error information
    """
    try:
        logger.info(f"Updating position stops for {symbol}")
        
        # Build parameters for set_trading_stop
        trading_stop_params = {
            "category": "linear",
            "symbol": symbol,
            "positionIdx": 0  # One-Way Mode
        }
        
        # Add stop loss if provided
        if stop_loss is not None:
            sl_str = f"{stop_loss:.4f}".rstrip('0').rstrip('.')
            trading_stop_params["stopLoss"] = sl_str
            logger.info(f"Setting Stop Loss: {sl_str}")
        
        # Add take profit if provided
        if take_profit is not None:
            tp_str = f"{take_profit:.4f}".rstrip('0').rstrip('.')
            trading_stop_params["takeProfit"] = tp_str
            logger.info(f"Setting Take Profit: {tp_str}")
        
        # Add trailing stop if provided
        if trailing_stop is not None:
            ts_str = f"{trailing_stop:.4f}".rstrip('0').rstrip('.')
            trading_stop_params["trailingStop"] = ts_str
            logger.info(f"Setting Trailing Stop: {ts_str}")
        
        # Check if at least one parameter is provided
        if len(trading_stop_params) <= 3:  # Only category, symbol, positionIdx
            error_msg = "No stop loss, take profit, or trailing stop provided for update"
            logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg
            }
        
        logger.info(f"Update parameters: {json.dumps(trading_stop_params)}")
        
        # Execute the update
        update_response = bybit_client.set_trading_stop(**trading_stop_params)
        
        # Check response
        if update_response.get('retCode') == 0:
            logger.info(f"Position stops updated successfully for {symbol}")
            return {
                'success': True,
                'symbol': symbol,
                'stop_loss': stop_loss,
                'take_profit': take_profit,
                'trailing_stop': trailing_stop
            }
        else:
            error_code = update_response.get('retCode')
            error_msg = update_response.get('retMsg', 'Unknown error')
            
            # Handle error 34040 (not modified) gracefully
            if error_code == 34040:
                logger.warning(f"Position stops not modified (error 34040) - values may already be set or identical")
                return {
                    'success': True,
                    'warning': "Position stops not modified - values may already be set",
                    'symbol': symbol
                }
            else:
                logger.error(f"Failed to update position stops - retCode: {error_code}, retMsg: {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'error_code': error_code,
                    'response': update_response
                }
        
    except Exception as e:
        logger.error(f"Error updating position stops: {str(e)}", exc_info=True)
        return {
            'success': False,
            'error': str(e)
        }


def execute_bybit_order(
    bybit_client: HTTP,
    action: str,
    symbol: str,
    quantity: float,
    stop_loss: Optional[float] = None,
    take_profit: Optional[float] = None,
    trailing_stop: Optional[float] = None
) -> Dict[str, Any]:
    """
    Execute order on Bybit Perpetual Futures (V5 API).
    
    Args:
        bybit_client: Bybit HTTP client instance
        action: Order action (Buy/Long or Sell/Short)
        symbol: Trading symbol
        quantity: Position quantity
        stop_loss: Stop loss price (optional)
        take_profit: Take profit price (optional)
        trailing_stop: Trailing stop value (optional)
        
    Returns:
        Dictionary with order result or error information
    """
    try:
        # Normalize action to Bybit side
        side = "Buy" if action.upper() in ["BUY", "LONG"] else "Sell"
        
        # ✅ Bybit V5: Convert quantity to string (avoid precision errors)
        qty_str = f"{quantity:.4f}".rstrip('0').rstrip('.')
        
        logger.info(f"Placing {side} order for {symbol} - Qty: {qty_str}")
        
        # ✅ Bybit V5: Place market order (category="linear", no price parameter)
        order_params = {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": "Market",
            "qty": qty_str,
            "positionIdx": 0,  # One-Way Mode
            "reduceOnly": False
        }
        
        logger.info(f"Order parameters: {json.dumps(order_params)}")
        
        order_response = bybit_client.place_order(**order_params)
        
        # ✅ Bybit V5: Enhanced error handling with retCode and retMsg
        if order_response.get('retCode') != 0:
            error_code = order_response.get('retCode')
            error_msg = order_response.get('retMsg', 'Unknown error')
            
            logger.error(f"Bybit order failed - retCode: {error_code}, retMsg: {error_msg}")
            
            return {
                'success': False,
                'error': error_msg,
                'error_code': error_code,
                'response': order_response
            }
        
        order_id = order_response.get('result', {}).get('orderId')
        order_link_id = order_response.get('result', {}).get('orderLinkId')
        
        logger.info(f"Order placed successfully - Order ID: {order_id}")
        
        # ✅ Bybit V5: Set stop loss, take profit, and/or trailing stop
        # Wait for position to be established
        time.sleep(1.0)
        
        sl_tp_warning = None  # Track SL/TP warnings to return to main handler
        
        if stop_loss or take_profit or trailing_stop:
            try:
                trading_stop_params = {
                    "category": "linear",
                    "symbol": symbol,
                    "positionIdx": 0
                }
                
                # ✅ Bybit V5: Convert SL/TP/TS to strings
                if stop_loss:
                    sl_str = f"{stop_loss:.4f}".rstrip('0').rstrip('.')
                    trading_stop_params["stopLoss"] = sl_str
                    logger.info(f"Setting Stop Loss: {sl_str}")
                
                if take_profit:
                    tp_str = f"{take_profit:.4f}".rstrip('0').rstrip('.')
                    trading_stop_params["takeProfit"] = tp_str
                    logger.info(f"Setting Take Profit: {tp_str}")
                
                if trailing_stop:
                    ts_str = f"{trailing_stop:.4f}".rstrip('0').rstrip('.')
                    trading_stop_params["trailingStop"] = ts_str
                    logger.info(f"Setting Trailing Stop: {ts_str}")
                
                logger.info(f"SL/TP/TS parameters: {json.dumps(trading_stop_params)}")
                
                sl_tp_response = bybit_client.set_trading_stop(**trading_stop_params)
                
                # ✅ Bybit V5: Check SL/TP response
                if sl_tp_response.get('retCode') == 0:
                    logger.info(f"Stop loss, take profit, and trailing stop set successfully")
                else:
                    error_code = sl_tp_response.get('retCode')
                    error_msg = sl_tp_response.get('retMsg', 'Unknown error')
                    
                    # Handle error 34040 (not modified) gracefully
                    if error_code == 34040:
                        logger.warning(f"SL/TP/TS not modified (error 34040) - values may already be set or identical to existing values")
                        sl_tp_warning = "SL/TP/TS not modified (34040) - values may already be set"
                    else:
                        logger.warning(f"Failed to set SL/TP/TS - retCode: {error_code}, retMsg: {error_msg}")
                        sl_tp_warning = f"SL/TP/TS setup failed: {error_msg} (Code: {error_code})"
                    
            except Exception as e:
                logger.warning(f"Error setting stop loss/take profit/trailing stop: {str(e)}")
                sl_tp_warning = f"SL/TP/TS setup exception: {str(e)}"
        
        # ✅ Bybit V5: Get fill price from execution history
        fill_price = None
        try:
            time.sleep(0.5)
            
            # ✅ Use category parameter for V5
            trade_response = bybit_client.get_executions(
                category="linear",
                symbol=symbol,
                limit=1
            )
            
            if trade_response.get('retCode') == 0:
                executions = trade_response.get('result', {}).get('list', [])
                if executions:
                    fill_price = float(executions[0].get('execPrice', 0))
                    logger.info(f"Fill price retrieved: {fill_price}")
            else:
                logger.warning(f"Could not retrieve fill price - retCode: {trade_response.get('retCode')}")
                    
        except Exception as e:
            logger.warning(f"Could not retrieve fill price: {str(e)}")
        
        return {
            'success': True,
            'order_id': order_id,
            'order_link_id': order_link_id,
            'fill_price': fill_price,
            'sl_tp_warning': sl_tp_warning  # Return warning to main handler
        }
        
    except Exception as e:
        logger.error(f"Error executing Bybit order: {str(e)}", exc_info=True)
        
        return {
            'success': False,
            'error': str(e)
        }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler function for TradingView webhook.
    
    Expected JSON payload from TradingView:
    
    For new orders:
    {
        "action": "buy",
        "symbol": "BTCUSDT",
        "qty": 0.5,
        "category": "linear",
        "sl": 42150.50,
        "tp": 43500.75,
        "trailing_stop": 100.0
    }
    
    For position updates:
    {
        "action": "update",
        "symbol": "BTCUSDT",
        "sl": 42500.00,
        "tp": 44000.00,
        "trailing_stop": 150.0
    }
    
    Args:
        event: Lambda event containing TradingView alert data
        context: Lambda context object
        
    Returns:
        HTTP response dictionary
    """
    # Initialize Telegram flag at the very beginning
    telegram_sent = False
    
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
        
        # ✅ Extract parameters (case-insensitive)
        action = body.get('action') or body.get('Action') or body.get('ACTION')
        symbol = body.get('symbol') or body.get('Symbol') or body.get('SYMBOL')
        qty = body.get('qty') or body.get('Qty') or body.get('QTY') or body.get('quantity')
        price = body.get('price') or body.get('Price') or body.get('entry')
        stop_loss = body.get('sl') or body.get('SL') or body.get('stopLoss')
        take_profit = body.get('tp') or body.get('TP') or body.get('takeProfit')
        trailing_stop = body.get('trailing_stop') or body.get('trailingStop') or body.get('ts')
        
        # Validate required fields
        if not action:
            error_msg = 'Missing required field: action'
            logger.error(error_msg)
            if not telegram_sent:
                send_telegram_message(f"❌ <b>Webhook Error:</b> {error_msg}")
                telegram_sent = True
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'success': False,
                    'error': error_msg
                })
            }
        
        if not symbol:
            error_msg = 'Missing required field: symbol'
            logger.error(error_msg)
            if not telegram_sent:
                send_telegram_message(f"❌ <b>Webhook Error:</b> {error_msg}")
                telegram_sent = True
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'success': False,
                    'error': error_msg
                })
            }
        
        # ✅ Normalize symbol format (Bybit V5 uses uppercase, no .P suffix)
        symbol = symbol.upper().replace('.P', '').replace('-', '')
        
        logger.info(f"Normalized symbol: {symbol}")
        
        # ✅ Initialize Bybit V5 client
        try:
            bybit_client = HTTP(
                testnet=BYBIT_TESTNET,
                api_key=BYBIT_API_KEY,
                api_secret=BYBIT_API_SECRET
            )
            logger.info(f"Bybit client initialized - Testnet: {BYBIT_TESTNET}")
        except Exception as e:
            error_msg = f"Failed to initialize Bybit client: {str(e)}"
            logger.error(error_msg)
            if not telegram_sent:
                send_telegram_error("Bybit Client Initialization Failed", {
                    "Error": str(e),
                    "Testnet": BYBIT_TESTNET
                })
                telegram_sent = True
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'success': False,
                    'error': error_msg
                })
            }
        
        # ========== NEW: HANDLE UPDATE ACTION ==========
        if action.upper() == "UPDATE":
            logger.info(f"Processing UPDATE action for {symbol}")
            
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
            
            try:
                ts_price = float(trailing_stop) if trailing_stop else None
            except (ValueError, TypeError):
                logger.warning(f"Invalid trailing stop value: {trailing_stop}")
                ts_price = None
            
            # Execute position update with error handling
            try:
                update_result = update_position_stops(
                    bybit_client=bybit_client,
                    symbol=symbol,
                    stop_loss=sl_price,
                    take_profit=tp_price,
                    trailing_stop=ts_price
                )
                
                if not update_result.get('success'):
                    error_msg = update_result.get('error', 'Unknown error')
                    error_code = update_result.get('error_code', 'N/A')
                    
                    if not telegram_sent:
                        send_telegram_error("Position Update Failed", {
                            "Symbol": symbol,
                            "Error Code": error_code,
                            "Error Message": error_msg
                        })
                        telegram_sent = True
                    
                    return {
                        'statusCode': 500,
                        'body': json.dumps({
                            'success': False,
                            'error': error_msg,
                            'error_code': error_code
                        })
                    }
                
                # Send success notification
                if not telegram_sent:
                    telegram_message = format_telegram_notification(
                        symbol=symbol,
                        action="UPDATE",
                        entry_price=0.0,  # Not applicable for updates
                        quantity=0.0,  # Not applicable for updates
                        stop_loss=sl_price,
                        take_profit=tp_price,
                        trailing_stop=ts_price
                    )
                    
                    # Add warning if present
                    if update_result.get('warning'):
                        telegram_message += f"\n\n⚠️ <i>{update_result['warning']}</i>"
                    
                    send_telegram_message(telegram_message)
                    telegram_sent = True
                
                # Return success response
                response_data = {
                    'success': True,
                    'action': 'update',
                    'symbol': symbol,
                    'stop_loss': sl_price,
                    'take_profit': tp_price,
                    'trailing_stop': ts_price,
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
                }
                
                logger.info(f"Position updated successfully: {json.dumps(response_data)}")
                
                return {
                    'statusCode': 200,
                    'body': json.dumps(response_data)
                }
                
            except Exception as update_error:
                error_msg = f"Exception during position update: {str(update_error)}"
                logger.error(error_msg, exc_info=True)
                
                if not telegram_sent:
                    send_telegram_error("Position Update Exception", {
                        "Symbol": symbol,
                        "Error": str(update_error)
                    })
                    telegram_sent = True
                
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'success': False,
                        'error': error_msg
                    })
                }
        
        # ========== EXISTING: HANDLE BUY/SELL ACTIONS ==========
        # Calculate quantity if not provided
        quantity = None
        if qty:
            try:
                quantity = float(qty)
                logger.info(f"Using provided quantity: {quantity}")
            except (ValueError, TypeError):
                logger.warning(f"Invalid qty value: {qty}, will calculate dynamically")
        
        if not quantity or quantity <= 0:
            if not price:
                error_msg = "Cannot calculate position size: price is required when qty is not provided"
                logger.error(error_msg)
                if not telegram_sent:
                    send_telegram_message(f"❌ <b>Error:</b> {error_msg}")
                    telegram_sent = True
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'success': False,
                        'error': error_msg
                    })
                }
            
            try:
                entry_price = float(price)
                quantity = calculate_position_size(bybit_client, symbol, entry_price)
                
                if not quantity or quantity <= 0:
                    error_msg = 'Failed to calculate position size'
                    logger.error(error_msg)
                    if not telegram_sent:
                        send_telegram_error("Position Size Calculation Failed", {
                            "Symbol": symbol,
                            "Price": entry_price
                        })
                        telegram_sent = True
                    return {
                        'statusCode': 500,
                        'body': json.dumps({
                            'success': False,
                            'error': error_msg
                        })
                    }
                    
            except (ValueError, TypeError) as e:
                error_msg = f'Invalid price value: {price}'
                logger.error(error_msg)
                if not telegram_sent:
                    send_telegram_message(f"❌ <b>Error:</b> {error_msg}")
                    telegram_sent = True
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'success': False,
                        'error': error_msg
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
        
        try:
            ts_price = float(trailing_stop) if trailing_stop else None
        except (ValueError, TypeError):
            logger.warning(f"Invalid trailing stop value: {trailing_stop}")
            ts_price = None
        
        logger.info(f"Final order parameters - Symbol: {symbol}, Action: {action}, Qty: {quantity}, SL: {sl_price}, TP: {tp_price}, TS: {ts_price}")
        
        # Execute order
        order_result = execute_bybit_order(
            bybit_client=bybit_client,
            action=action,
            symbol=symbol,
            quantity=quantity,
            stop_loss=sl_price,
            take_profit=tp_price,
            trailing_stop=ts_price
        )
        
        if not order_result.get('success'):
            error_msg = order_result.get('error', 'Unknown error')
            error_code = order_result.get('error_code', 'N/A')
            
            if not telegram_sent:
                send_telegram_error("Order Execution Failed", {
                    "Symbol": symbol,
                    "Action": action,
                    "Quantity": quantity,
                    "Error Code": error_code,
                    "Error Message": error_msg
                })
                telegram_sent = True
            
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'success': False,
                    'error': error_msg,
                    'error_code': error_code
                })
            }
        
        # Get fill price (use provided price as fallback)
        fill_price = order_result.get('fill_price')
        if not fill_price and price:
            try:
                fill_price = float(price)
            except (ValueError, TypeError):
                fill_price = 0.0
        
        # Send success notification
        if not telegram_sent:
            telegram_message = format_telegram_notification(
                symbol=symbol,
                action=action,
                entry_price=fill_price or 0.0,
                quantity=quantity,
                stop_loss=sl_price,
                take_profit=tp_price,
                order_id=order_result.get('order_id'),
                trailing_stop=ts_price
            )
            
            # Add SL/TP warning if present
            sl_tp_warning = order_result.get('sl_tp_warning')
            if sl_tp_warning:
                telegram_message += f"\n\n⚠️ <i>{sl_tp_warning}</i>"
            
            send_telegram_message(telegram_message)
            telegram_sent = True
        
        # Return success response
        response_data = {
            'success': True,
            'symbol': symbol,
            'action': action,
            'quantity': quantity,
            'order_id': order_result.get('order_id'),
            'fill_price': fill_price,
            'stop_loss': sl_price,
            'take_profit': tp_price,
            'trailing_stop': ts_price,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
        }
        
        logger.info(f"Order executed successfully: {json.dumps(response_data)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(response_data)
        }
        
    except json.JSONDecodeError as e:
        error_msg = f'Invalid JSON: {str(e)}'
        logger.error(error_msg)
        if not telegram_sent:
            send_telegram_message(f"❌ <b>JSON Parse Error:</b> {error_msg}")
            telegram_sent = True
        return {
            'statusCode': 400,
            'body': json.dumps({
                'success': False,
                'error': error_msg
            })
        }
        
    except Exception as e:
        error_msg = f'Internal server error: {str(e)}'
        logger.error(error_msg, exc_info=True)
        
        if not telegram_sent:
            send_telegram_error("Unexpected Lambda Error", {
                "Error": str(e),
                "Type": type(e).__name__
            })
            telegram_sent = True
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': error_msg
            })
        }