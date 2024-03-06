# liquidity-break
This is an implementation of an indicator which shows the liquidity breaks within an OHLCV dataset. 
Original indicator is written by TradingView's user ChartPrime using Pinescript language.
It is based on "Trend Channels With Liquidity Breaks" indicator of ChartPrime, available at TradingView.

# Indicator's source:
[Trend Channels With Liquidity Breaks (ChartPrime)](https://www.tradingview.com/v/34t0EaMk/)
   
# Installation
Python version >= 3.10 is required.  
  
`pip3 install -r requirements.txt`

# Usage:
```python
# Initiate bybit connector with ccxt to get OHLCV data
bybit = ccxt.bybit()
symbol = "ETHUSDT"

# Indicator params
length = 8
extend = False

# Candle timeframe
tf = "30m"
tf_min = 30

# Pull data from ccxt and format dataframe
df = pd.DataFrame(bybit.fetch_ohlcv(symbol, timeframe= tf, limit= 200), columns= ["time", "open", "high", "low", "close", "volume"])
df['datetime'] = pd.to_datetime(df['time'], unit="ms")
df['dt'] = df['datetime'].copy()
df = df.iloc[:-1]

# Get last liquidity break
last_break = find_breaks(df, length, extend, tf_min)
```
