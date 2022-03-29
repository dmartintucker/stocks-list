# Dependencies
import numpy as np
import pandas as pd
import warnings
import yfinance as yf 

warnings.filterwarnings('ignore')



class YahooFinance:

    def __init__(self) -> None:
        pass

    def get_history(self, start, end, ticker):
        
        history = yf.Ticker(ticker).history(start=start, end=end, freq='1D')
        return history

    def get_info(self, ticker, debug=True) -> dict:

        info = {}
        info['symbol'] = ticker

        try:
            data = yf.Ticker(ticker).info
            info['industry'] = data['industry']
            info['sector'] = data['sector']
            info['name'] = data['shortName']
            info['quoteType'] = data['quoteType']

        except:
            info['industry'] = np.nan
            info['sector'] = np.nan
            info['name'] = np.nan
            info['quoteType'] = np.nan

        if debug and not pd.isnull(info['quoteType']):
            print(f"Found a security for the symbol '{ticker}'")
            
        return info