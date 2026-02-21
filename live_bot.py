import os
import time
import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

# 1. Securely load the keys
load_dotenv()
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")

trading_client = TradingClient(api_key=API_KEY, secret_key=API_SECRET, paper=True)

# 2. Strategy Parameters
ASSET1 = 'JPM'
ASSET2 = 'BAC'
BETA = 0.5716  
WINDOW = 30           # UPGRADED: Mathematically optimized from 20 to 30
TRADE_QTY = 10 
ENTRY_THRESHOLD = 2.0 # UPGRADED: Verified optimal threshold
TAKE_PROFIT_THRESHOLD = 0.5
STOP_LOSS_THRESHOLD = 3.5

# --- NEW: Institutional Thresholds ---
ENTRY_THRESHOLD = 2.0
TAKE_PROFIT_THRESHOLD = 0.5
STOP_LOSS_THRESHOLD = 3.5  # If Z-score hits +/- 3.5, the rubber band is broken

def get_current_z_score():
    """Fetches market data and calculates the live Z-Score."""
    # UPGRADED: Fetches 3 months of data to ensure we have enough days for a 30-day window
    data = yf.download([ASSET1, ASSET2], period='3mo', progress=False)['Close'].dropna()
    log_data = np.log(data)
    spread = log_data[ASSET1] - (BETA * log_data[ASSET2])
    
    rolling_mean = spread.rolling(window=WINDOW).mean()
    rolling_std = spread.rolling(window=WINDOW).std()
    
    z_score = (spread - rolling_mean) / rolling_std
    return z_score.iloc[-1]

def check_open_positions():
    """Queries Alpaca to see if we are currently in a trade."""
    positions = trading_client.get_all_positions()
    open_symbols = [p.symbol for p in positions]
    # Return True if either asset is currently in our portfolio
    return (ASSET1 in open_symbols) or (ASSET2 in open_symbols)

def execute_trade(symbol, side, qty):
    """Sends a market order."""
    try:
        market_order_data = MarketOrderRequest(
            symbol=symbol, qty=qty, side=side, time_in_force=TimeInForce.DAY
        )
        order = trading_client.submit_order(order_data=market_order_data)
        print(f"✅ EXECUTED: {side.name} {qty} shares of {symbol}")
    except Exception as e:
        print(f"❌ ORDER FAILED for {symbol}: {e}")

# 3. The Autonomous Event Loop
if __name__ == "__main__":
    print("--- Matfyz Statistical Arbitrage Bot v2.0 (Institutional) ---")
    print("Initializing Risk Management Protocols...\n")
    
    while True:
        try:
            print(f"[{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}] Scanning market...")
            
            # Gather State and Data
            current_z = get_current_z_score()
            in_trade = check_open_positions()
            
            print(f"Live Z-Score: {current_z:.4f} | In Trade: {in_trade}")
            
            # --- THE DECISION MATRIX ---
            
            # SCENARIO A: We are currently IN a trade
            if in_trade:
                # 1. Stop-Loss Check (Emergency Liquidation)
                if abs(current_z) >= STOP_LOSS_THRESHOLD:
                    print("🚨 REGIME CHANGE DETECTED: Z-Score exceeded 3.5!")
                    print("Executing Emergency Stop-Loss. Liquidating all positions.")
                    trading_client.close_all_positions(cancel_orders=True)
                
                # 2. Take Profit Check (Mean Reversion Achieved)
                elif abs(current_z) <= TAKE_PROFIT_THRESHOLD:
                    print("💰 TAKE PROFIT: Rubber band snapped back. Closing positions.")
                    trading_client.close_all_positions(cancel_orders=True)
                
                else:
                    print("⏳ Holding current positions. Waiting for target.")

            # SCENARIO B: We are NOT in a trade (Looking for entry)
            else:
                if current_z > ENTRY_THRESHOLD:
                    print("🚨 ENTRY: SHORT the Spread!")
                    execute_trade(ASSET1, OrderSide.SELL, TRADE_QTY)
                    execute_trade(ASSET2, OrderSide.BUY, int(TRADE_QTY * BETA))
                
                elif current_z < -ENTRY_THRESHOLD:
                    print("🚨 ENTRY: LONG the Spread!")
                    execute_trade(ASSET1, OrderSide.BUY, TRADE_QTY)
                    execute_trade(ASSET2, OrderSide.SELL, int(TRADE_QTY * BETA))
                
                else:
                    print("🟢 SIGNAL: FLAT. No anomaly detected.")
            
            print("Sleeping for 1 minute...\n")
            time.sleep(60)
            
        except KeyboardInterrupt:
            print("\n🛑 Bot manually stopped by user. Shutting down gracefully.")
            break
        except Exception as e:
            print(f"⚠️ Error in main loop: {e}")
            time.sleep(60)