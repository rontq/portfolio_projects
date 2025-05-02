import yfinance as yf

nvda = yf.Ticker("NVDA")
print(nvda.history(period="1d"))