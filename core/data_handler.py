import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals
import yfinance as yf

from scrape_finance.spiders.ecb_daily import EcbDailySpider
from scrape_finance.spiders.ecb_hist import EcbHistSpider

import requests
import os
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
ALPHA_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
class CurrencyCollector:
    """activates a Scrapy Crawler process to get either current date or 
     historical EUR/USD quotes from the ECB official website
      args: hist:bool. Default=False, for current quote
       returns: 
        a list of historical quotes or a dict with current date quote """
    def __init__(self, hist:bool=False):
        self.currency_results = []
        self.daily_currency_results = None
        self.process = CrawlerProcess()
        self.hist = hist
        self.currency_df = None
        self.start_date = None


    def _collect_hist_item(self, item):
        if self.hist:
            try:
                datetime_obj = datetime.strptime(item['date'],"%Y-%m-%d").date()
                if datetime_obj >= self.start_date:
                    item['date'] = datetime_obj
                    item['value'] = float(item['value'])
                    self.currency_results.append(item)
            except KeyError: 
                pass

    def hist_scraper(self):
        # set start date 
        today = datetime.today().date()
        self.start_date = today.replace(year=(today.year -2 ))
        
        # set callback and callback signal
        dispatcher.connect(self._collect_hist_item, signal=signals.item_scraped)
        
        #start crawl:
        self.process.crawl(EcbHistSpider)

    def _collect_daily_currency(self, item):
        item['date'] = datetime.strptime(item['date'],"%d %B %Y").date()
        self.currency_results.append(item)
    
    def daily_scraper(self):
        # set callback and callback signal
        dispatcher.connect(self._collect_daily_currency, signal=signals.item_scraped)
        
        #start crawl:
        self.process.crawl(EcbDailySpider)

    def run(self):
        if self.hist:
            self.hist_scraper()
        else: 
            self.daily_scraper()
        self.process.start()

        # set dataframe:
        self.currency_df = pd.DataFrame(data=self.currency_results)
        self.currency_df.set_index('date', inplace=True)
        self.currency_df.index = pd.to_datetime(self.currency_df.index)
        self.currency_df.sort_index(ascending=False, inplace=True)
        self.currency_df.rename(columns={'value': 'usd_value'}, inplace=True)



class BrentPriceCollector:
    """collects historical or current commodity prices for Brent Crude Oil, 
    using Alpha vantage API.
          args: 
          api_key: str Alpha Vantage API Key
          hist:bool. Default=False, for current quote
    returns: 
        a dict of historical quotes per date or a dict with current date quote """

    def __init__(self, api_key, hist:bool=False):
        self.api_key = api_key
        self.hist = hist
        self.json_data = None
        self.brent_raw_data = None
        self.start_date = None
        self.brent_df = None

    def get_brent_quotes(self):

        # call Alpha Vantage 'BRENT' function api with single day interval: 
        url = f'https://www.alphavantage.co/query?function=BRENT&interval=daily&apikey={self.api_key}'
        r = requests.get(url)
        self.data = r.json()
        
        # choose historical or current trading data
        self.brent_raw_data = self.data['data'] 

        # convert to Pandas df with date as index, filter for 2 years: 
        if self.hist: 
            
            today = pd.to_datetime(datetime.today().date())
            self.start_date = today - pd.DateOffset(years=2)
            values = [item['value'] for item in self.brent_raw_data]
            pd_dates = pd.to_datetime([item['date'] for item in self.brent_raw_data])
            brent_df = pd.DataFrame(
                        data={'date': pd_dates, 'brent_value': values},
                        index=pd_dates)
            
            # sort out last 2 years
            self.brent_df = brent_df.loc[brent_df.index >= self.start_date]
            
            # convert to float, handle non-numeric as 'NaN'
            self.brent_df['brent_value'] = pd.to_numeric(self.brent_df['brent_value'], errors='coerce')
        else: 
            last_day_value = self.brent_raw_data[0]
            value = float(last_day_value['value'])
            date = pd.to_datetime(last_day_value['date'])
            self.brent_df = pd.DataFrame(
                data={'date':date,'brent_value': value},
                index=[date]  
            )
            
        self.brent_df.set_index('date', inplace=True)
        self.brent_df.index = pd.to_datetime(self.brent_df.index)

class OilForwardValueCollector:
    """
    collects values for the combined index of close-value of Brent-Crude Future transactions
    args: 
          hist:bool. Default=False, for current quote, true for historical
    returns: 
        a dict of historical quotes per date or a dict with current date quote """
    
    def __init__(self, hist:bool = False):
        self.ticker = 'BZ=F'
        self.hist = hist
        self.oil_future_df = None
        self.start_date = None
    
    def collect_values(self):

        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0'
        })    

        if self.hist: 
            # set start date to 2 years past: 
            today = datetime.today().date()
            self.start_date = today.replace(year=(today.year -2 ))
            
            # get quotes
            data = yf.download(self.ticker, start=self.start_date, end=today, interval="1d")
        
        else:
            # get data for last trading day: 
            data =  yf.download(self.ticker, period='1d', session=session, interval='1d')
            print(f'data from yf.download oil futures: {data}')
        
        # rename columns to <value>_<ticker>; lower case only first letter:
        data.columns = data.columns.map(lambda col: ('_'.join(col)))
        data.rename(columns={f'Close_{self.ticker}': f'close_{self.ticker}'}, inplace=True)
        self.oil_future_df = data[[f'close_{self.ticker}']]
        self.oil_future_df.index.name ='date'
        self.oil_future_df.sort_index(ascending=False, inplace=True)



class SentimentIndexCollector:
    """
    collects values for the combined index of close-value of Brent-Crude Future transactions
    args: 
          hist:bool. Default=False, for current quote, true for historical
    returns: 
        a dict of historical quotes per date or a dict with current date quote """
    
    def __init__(self, hist:bool = False):
        self.ticker = '^VIX'
        self.hist = hist
        self.vix = None
        self.start_date = None
        self.vix_df = None
    
    def collect_values(self):
        
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0'
        }) 
        if self.hist: 
            # set start date to 2 years past: 
            today = datetime.today().date()
            self.start_date = today.replace(year=(today.year -2 ))
            
            # get quotes
            data = yf.download(self.ticker, start=self.start_date, end=today, interval="1d")
        
        else:
            # get data for last trading day: 
            data =  yf.download(self.ticker, period='1d', interval='1d')
            print(f'data from yf.download vix sentiment: {data}')

        #rename df columns
        data.columns = data.columns.map(lambda col: '_'.join(col))
        data.rename(columns={f'Close_{self.ticker}': f'close_{self.ticker}'}, inplace=True)

        #get only close price column
        self.vix_df = data[[f'close_{self.ticker}']]
        self.vix_df.index.name ='date'
        self.vix_df.sort_index(ascending=False, inplace=True)

def scrape_factory(alpha_api_key, hist=False)->list:
    """
    fetches all values for either historical or current.
    currently - joins all value to a dataframe. 
    arg: hist: Boolean, default False. False returns current values
     True to return historical values
    """

    # fetch usd/eur currency data:
    t0 = time.perf_counter()
    usd = CurrencyCollector(hist=hist)
    usd.run()
    usd_df = usd.currency_df
    print(f"usd data elapsed time: {time.perf_counter() - t0:.2f} seconds")
    
    # fetch Brent index for oil prices: 
    t1 = time.perf_counter()
    brent = BrentPriceCollector(alpha_api_key, hist=hist)
    brent.get_brent_quotes()
    brent_df = brent.brent_df
    print(f"brent data elapsed time: {time.perf_counter() - t1:.2f} seconds")

    # fetch future oil transacion index prices:
    t2 = time.perf_counter()
    oil = OilForwardValueCollector(hist=hist)
    oil.collect_values()
    oil_df = oil.oil_future_df
    print(f"BZ future data elapsed time: {time.perf_counter() - t2:.2f} seconds")

    # fetch general 'fear' market sentiment index prices:
    t3 = time.perf_counter()
    vix = SentimentIndexCollector(hist=hist)
    vix.collect_values()
    vix_df = vix.vix_df
    print(f"VIX future data elapsed time: {time.perf_counter() - t3:.2f} seconds")

    return [{
        'name': 'currency','df': usd_df}, 
            {'name':'brent', 'df': brent_df},
            {'name':'BZ_oil', 'df': oil_df},
            {'name':'vix', 'df': vix_df}
            ]
    

def save_to_hdf(df_dict, hist:bool):
    """saves market data to hd5 file"""

    name = df_dict['name']
    key=f'/{name}'
    df = df_dict['df']
    path = ('./data/oil_market_data.h5')
    mode = 'a'
    
    if hist:
        # check if each of the keys exist in datastore- 
        
        with pd.HDFStore(path, mode=mode) as store:
            if key in store:
                print(f'data file for {key} already exists. cannot recreate, please collect current data only with "hist=False"')    
                return
            store.put(key=key, value=df, format='table')
            print(f'key {key}\n df: {df}')
        
    else:
        if os.path.exists(path):
            with pd.HDFStore(path, mode=mode) as store:
                store.append(key, df, format='table')

        else: 
            print('data file does not exist. check and initialize with "hist=True"')
            return

def scrape_orchestrator(alpha_api_key, hist: bool=False):
    df_list  = scrape_factory(alpha_api_key, hist=hist)
    for df_dict in df_list:
        save_to_hdf(df_dict, hist=hist)

    return df_list

def load_from_hdf(path='./data/oil_market_data.h5'):
    df_list = []
    
    with pd.HDFStore(path, mode='r') as store: 
        for key in store.keys():
            df = store[key]
            df.index.drop_duplicates(keep='first')
            #df = df.convert_dtypes(convert_floating=True)
            df_list.append(df)
    
    return df_list
    

    
if __name__ == "__main__":
    scrape_orchestrator(hist=False)
