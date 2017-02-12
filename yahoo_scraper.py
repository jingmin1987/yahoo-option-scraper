# -*- coding: utf-8 -*-
"""
Created on Sun Feb  5 18:19:31 2017

@author: Jingmin Zhang

"""

import pandas as pd
import os
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys


class YahooScraper:
    """Yahoo Finance Option Scraper"""
    
    def __init__(self, symbols, max_tries=5, explicit_wait=5, ext_path=None):
        # Parameters
        if not isinstance(symbols, list):
            self.symbols = [symbols]
        else:
            self.symbols = symbols
            
        self.max_tries = max_tries
        self.explicit_wait = explicit_wait
        self.ext_path = None
        
        # Results
        self.data = pd.DataFrame()
        self.timer = {
            'Browser Open': [],
            'Page Load': [],
            'DF Parse': [],
            'Total Time': []      
        }
        
        # Intermediate parameters
        self.expiration_dates = {}
        for symbol in self.symbols:
            self.expiration_dates[symbol] = []
            
        self.browser = None
        self.profile = webdriver.ChromeOptions()
        self.new_symbol = None # In case if a symbol has been changed
        
        if ext_path != None:
            if os.path.isfile(str(ext_path)):
                self.ext_path = ext_path
                self.profile.add_extension(ext_path)
            else:
                print('File specified by ext_path does not exit!')

    def _check_url(self, url):
        """Check if current url is as expected. Consumes 1 life if not"""
        
        return self.browser.current_url == url
    
    def _wait_for_element(self, xpath_str):
        """Wait for specified element to load in WebDriver"""
        
        element = None
        try:
            element = WebDriverWait(
                self.browser, self.explicit_wait).until(
                EC.presence_of_element_located((By.XPATH, xpath_str))
            )
        except:
            raise ElementNotFoundError
        finally:
            return element
   
    def _wait_for_elements(self, xpath_str):
        """Wait for specified elements to load in WebDriver"""
        
        elements = None
        try:
            elements = WebDriverWait(
                self.browser, self.explicit_wait).until(
                EC.presence_of_all_elements_located((By.XPATH, xpath_str))
            )
        except:
            raise ElementNotFoundError
        finally:
            return elements
            
    def _get_expiration_dates(self, symbol, browser_quit=False):
        """Scrape the expiration dates for all available options"""
        
        url = ('http://finance.yahoo.com/quote/'
               + symbol + '/options?p=' + symbol)
        
        # Initiate a browser if there's none
        if self.browser is None:
            timer = Timer() # Time browser opening
            self.browser = webdriver.Chrome(chrome_options=self.profile)
            self.timer['Browser Open'].append(timer.stop())
        
        timer = Timer() # Time page loading
        self.browser.get(url)
        
        if self._check_url(url):
            xpath_str = ('//*[@id="main-0-Quote-Proxy"]/section/div[2]/'
                         'section/div/section/div[2]/div[1]/select/'
                         'option[text()]')
            
            elements = self._wait_for_elements(xpath_str)
            self.timer['Page Load'].append(timer.stop())
            
            if elements is not None:
                self.expiration_dates[symbol] = [
                    (x.get_attribute('value'), x.text) for x in elements
                ]
            else: # Empty page for a dead symbol
                raise ElementEmptyError
        else:
            raise SymbolNotFoundError('Page not found')
            
    def _try_redirect(self, symbol):
        """Try to redirect the URL to the right one with updated symbol"""
        
        if self.browser.current_url.find('lookup') > -1: # If a lookup page
            xpath_str = ('//*[@id="lookup-page"]/div/div/div'
                         + '/div/div/fieldset/input')
            element = self._wait_for_element(xpath_str)
            element.send_keys(symbol)
            element.send_keys(Keys.ENTER)
            time.sleep(5)
            
            if self.browser.current_url.find('quote') > -1: # if a quote page
                url = self.browser.current_url
                return (url[url.find('p=') + 2:], True)
            else:
                raise SymbolNotFoundError('Symbol not found after redirect')
        else:
            return (symbol, False) # 404 Page
            
    def _try_refresh_element(self, xpath_str):
        """Try to refresh the page in case some element refuses to render"""
        
        element, i = None, 0
        while (element is None) and (i < self.max_tries):
            element = self._wait_for_element(xpath_str)
            i += 1
            self.browser.refresh()
        
        if element is None:
            raise ElementEmptyError
            
        return element
        
    def _try_refresh_elements(self, xpath_str):
        """Try to refresh the page in case some elements refuse to render"""
        
        elements, i = None, 0
        while (elements is None) and (i < self.max_tries):
            elements = self._wait_for_elements(xpath_str)
            i += 1
            self.browser.refresh()
        
        if elements is None:
            raise ElementEmptyError
            
        return elements
    
    def scrape_one_stock(self, symbol, browser_quit=True):
        """Yahoo Finance Option Scraper"""
        
        yahoo_symbol = symbol
        
        # Initiate a browser if there's none
        if self.browser is None:
            timer = Timer() # Time browser opening
            self.browser = webdriver.Chrome(chrome_options=self.profile)
            self.timer['Browser Open'].append(timer.stop())
        
        # Scrape expiration dates if not available and check validity of symbol
        if not len(self.expiration_dates[yahoo_symbol]):
            status, i = False, 0
            while (not status) and (i < self.max_tries):
                try:
                    self._get_expiration_dates(yahoo_symbol)
                    status = True
                except:
                    symbol_tmp, status = self._try_redirect(yahoo_symbol)
                    if status:
                        yahoo_symbol = symbol_tmp
                        self._get_expiration_dates(yahoo_symbol)
                i += 1
            if not status: # Got only 404 pages
                raise Page404Error
        
        # Generate time marks
        now = datetime.now()
        dl_time = str(now.time())
        dl_date = str(now.date())
        dl_datetime = str(now)
        
        # Crawl and scrape each page
        for date in self.expiration_dates[yahoo_symbol]:
            expiration_date = date[0]
            url = ('http://finance.yahoo.com/quote/' + yahoo_symbol + 
                   '/options?p=' + yahoo_symbol + '&date=' + expiration_date)
            timer = Timer() # Time page loading
            self.browser.get(url)
            
            # Handle random 404 pages
            if not self._check_url(url):
                status, i = False, 0
                while (not status) and (i < self.max_tries):
                    i += 1
                    self.browser.get(url)
                    if self._check_url(url):
                        status = True
                if not status: # Got only 404 pages
                    raise Page404Error
                        
            # Current price
            xpath_str = ('//*[@id="quote-header-info"]/div[2]/'
                         'div[1]/div/span[1]')
            
            element = self._try_refresh_element(xpath_str)
            
            price = element.text
                    
            # Current Yahoo time
            xpath_str = '//*[@id="quote-market-notice"]/span'
            element = self._try_refresh_element(xpath_str)
            
            yahoo_time = element.text
                            
            # Current option prices
            xpath_str = ('//*[@id="main-0-Quote-Proxy"]/section/div[2]/'
                         'section/div/section/section[1]/table/tbody/tr[1]')
            element = self._try_refresh_element(
                         xpath_str) # Just to make sure the content is loaded
            
            self.timer['Page Load'].append(timer.stop())
            
            timer = Timer() # Time table parsing
            dfs = pd.read_html(self.browser.page_source)
            self.timer['DF Parse'].append(timer.stop())
            
            for df in dfs:
                # Modify and append data to self.data
                if df.shape[1] == 10:
                    # Determine option type
                    idx = df['Contract Name'][0].find('C')
                    option_type = 'Put'
                    if idx > 5:
                        option_type = 'Call'
                        
                    df['Expiration Date'] = date[1]
                    df['Download Time'] = dl_time
                    df['Download Date'] = dl_date
                    df['Download DateTime'] = dl_datetime
                    df['Download Source'] = 'Yahoo Finance'
                    df['Price @ DL Time'] = price
                    df['Yahoo Time'] = yahoo_time
                    df['Symbol'] = symbol
                    df['Yahoo Symbol'] = yahoo_symbol
                    df['Option Type'] = option_type
        
                    self.data = self.data.append(df, ignore_index=True)
       
        if browser_quit:
            self.browser.quit()
            self.browser = None
            
    def scrape_all(self, browser_quit=True):
        """Scrape data for all stocks from Yahoo Finance"""
        
        for symbol in self.symbols:
            timer = Timer() # Time each iteration
            try_counter = 0
            while try_counter < self.max_tries:
                try:
                    self.scrape_one_stock(symbol, browser_quit=False)
                    break
                except ElementNotFoundError:
                    try_counter += 1
                    self.browser.refresh()
                    time.sleep(2) # Sometimes certain elements get stuck to be
                                  # loaded. Wait a few seconds
                except (SymbolNotFoundError, Page404Error):
                    print(symbol + ' was not found.')
                    break
                except ElementEmptyError:
                    print(symbol + ' has no option data')
                    break
                
            if try_counter == self.max_tries:
                print('Max tries reached. No data is available for '
                      'symbol ' + symbol)
                
            self.timer['Total Time'].append(timer.stop())
                
        if browser_quit:
            self.browser.quit()
            self.browser = None
            
    def save_to_csv(self, file_path):
        """Save dataframe to a flat file"""
        
        self.data.to_csv(file_path)
        
    def save_to_sqlite(self, name, conn, if_exists='append', index=False):
        """Save datafrom to sqlite3"""
        
        self.data.to_sql(name, conn, if_exists=if_exists, index=index)

class Timer():
    def __init__(self):
        self.start = time.clock()
        
    def stop(self):
        return time.clock() - self.start
        
class SymbolNotFoundError(Exception):
    pass  
                 
class Page404Error(Exception):
    pass

class ElementNotFoundError(Exception):
    pass

class ElementEmptyError(Exception):
    pass
                    
            