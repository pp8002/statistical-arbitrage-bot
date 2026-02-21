import time
import os
import time
import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

# 1. Securely load the keys from the .env vault
load_dotenv()
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")

# Initialize the Alpaca Client
trading_client = TradingClient(api_key=API_KEY, secret_key=API_SECRET, paper=True)

# 2. Strategy Parameters
ASSET1 = 'JPM'
ASSET2 = 'BAC'
BETA = 0.5716  
WINDOW = 20    
TRADE_QTY = 10 # Base number of shares to trade

def get_current_z_score():
    """Fetches market data and calculates the live Z-Score."""
    data = yf.download([ASSET1, ASSET2], period='1mo', progress=False)['Close'].dropna()
    log_data = np.log(data)
    spread = log_data[ASSET1] - (BETA * log_data[ASSET2])
    
    rolling_mean = spread.rolling(window=WINDOW).mean()
    rolling_std = spread.rolling(window=WINDOW).std()
    
    z_score = (spread - rolling_mean) / rolling_std
    return z_score.iloc[-1]

def execute_trade(symbol, side, qty):
    """Constructs and sends a market order to Alpaca."""
    try:
        market_order_data = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=side,
            time_in_force=TimeInForce.DAY
        )
        # THIS IS THE TRIGGER PULL
        order = trading_client.submit_order(order_data=market_order_data)
        print(f"✅ EXECUTED: {side.name} {qty} shares of {symbol} (Order ID: {order.id})")
    except Exception as e:
        print(f"❌ ORDER FAILED for {symbol}: {e}")

# 3. The Autonomous Event Loop
if __name__ == "__main__":
    print("--- Matfyz Statistical Arbitrage Bot Initiated ---")
    print(f"Target Pair: {ASSET1} & {ASSET2} | Beta: {BETA}\n")
    
    while True:
        try:
            print(f"[{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}] Scanning market...")
            current_z = get_current_z_score()
            print(f"Live Z-Score: {current_z:.4f}")
            
            # The Decision Matrix
            if current_z > 2.0:
                print("🚨 SIGNAL: SHORT the Spread!")
                execute_trade(ASSET1, OrderSide.SELL, TRADE_QTY)
                execute_trade(ASSET2, OrderSide.BUY, int(TRADE_QTY * BETA))
            
            elif current_z < -2.0:
                print("🚨 SIGNAL: LONG the Spread!")
                execute_trade(ASSET1, OrderSide.BUY, TRADE_QTY)
                execute_trade(ASSET2, OrderSide.SELL, int(TRADE_QTY * BETA))
            
            else:
                print("🟢 SIGNAL: FLAT. No action taken.")
            
            # Put the bot to sleep for 60 seconds before checking again
            print("Sleeping for 1 minute...\n")
            time.sleep(60)
            
        except KeyboardInterrupt:
            print("\n🛑 Bot manually stopped by user. Shutting down gracefully.")
            break
        except Exception as e:
            print(f"⚠️ Error in main loop: {e}")
            time.sleep(60) # If there's a network error, sleep and try again