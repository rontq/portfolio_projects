from yahooquery import Ticker

nvda = Ticker("NVDA")
print(nvda.history(period="1d"))
