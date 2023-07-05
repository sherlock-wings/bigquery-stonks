import bs4 as bs
from datetime import datetime, timezone
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import pandas as pd
from re import sub, match
import requests
from tqdm import tqdm
from yfinance import Ticker



## API PARAMS


api_params = {
    'key':'RSK',
    'url':"https://realstonks.p.rapidapi.com/"
}

api_headers = {
    "X-RapidAPI-Key": os.environ.get(api_params['key']), 
	"X-RapidAPI-Host": api_params['url'][8:-1]
}



## GOOGLE BIGQUERY PARAMS


sp500_params_env = os.environ.get('SP500_PARAMS')
sp500_params = {
        'keypath': sp500_params_env.split(';')[0],
        'project_id':sp500_params_env.split(';')[1],
        'dataset_id':sp500_params_env.split(';')[2],
        'table_name':sp500_params_env.split(';')[3],
        'table_id':sp500_params_env.split(';')[4],
        'scope_url':'https://www.googleapis.com/auth/cloud-platform'
        }

mc_params_env = os.environ.get('MC_PARAMS')
mc_params = {
        'keypath': mc_params_env.split(';')[0],
        'project_id':mc_params_env.split(';')[1],
        'dataset_id':mc_params_env.split(';')[2],
        'table_name':mc_params_env.split(';')[3],
        'table_id':mc_params_env.split(';')[4],
        'scope_url':'https://www.googleapis.com/auth/cloud-platform'
        }



## FUNCTIONS


# get all the ticker symbols and stock names in the S&P 500 
def get_sp500():
    # retrieve contents of Wikipedia's S&P 500 List Webpage
    resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    
    # parse the webpage contents using lxml library
    soup = bs.BeautifulSoup(resp.text, 'lxml')

    # find the first object in the `wikitable sortable` class
    table = soup.find('table', {'class': 'wikitable sortable'})
    
    # for each row in the `wikitable sortable` object found, add the cotents from cols 1 and 2 to lists
    stocks = []
    for row in table.findAll('tr')[1:]:
        stock_info = (row.findAll('td')[0].text,
                      row.findAll('td')[1].text,
                      row.findAll('td')[2].text,
                      row.findAll('td')[3].text,
                      row.findAll('td')[4].text)
        
        # remove trailing newline chars
        stock_info = [sub('^(.+)\n+$', '\g<1>', item) if match('^.+\n+$', item)\
                      else item for item in stock_info]
        stocks.append(stock_info)
    return stocks

def cache_market_caps(ticker_symbol_list:list) -> pd.DataFrame:
    # check to see if data was already collected today
    files = os.listdir()
    csv_found = False
    old_csv_detected=False
    csv_filename = ''
    
    # check if any csv files already exist, and capture that file's name if so
    for f in files:
        if f.endswith('.csv'):
            csv_found = True
            csv_filename+=f
            break
    # if you did find a csv, check to see if its filename contains today's date
    if csv_found and csv_filename.split('_') != datetime.today().strftime('%Y-%m-%d'):
        old_csv_detected = True

    # if an old csv was found, delete it
    if old_csv_detected:
        os.remove(csv_filename)

    # write a new csv so long as no csv was found, or if one was found that was old
    if not csv_found or (csv_found and old_csv_detected):    
        data = []
        # call YFinance API to get all market caps for all S&P 500 stocks
        for stock in ticker_symbol_list: 
            ticker = Ticker(stock)
            info = ticker.info
            market_cap = info["marketCap"]
            data.append({'ticker_symbol':stock.replace('-','.'),'market_cap':market_cap})
        data = pd.DataFrame(data)
        file_name = datetime.today().strftime('%Y-%m-%d')
        data.to_csv(file_name+'_market_cap.csv', index=False)
        return file_name
    else:
        return None
    
    

# get a single row of data for a specific stock
def get_row(headers:dict, url:str, stk:tuple):
    # run API call
    row = requests.get(url+stk[0], headers=api_headers).json()

    # capture the time the API call was made (without decimal seconds)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Format without decimal seconds
    now = datetime.strptime(now, '%Y-%m-%d %H:%M:%S')  # Parse formatted string
    row['call_at'] = now
    
    # capture the ticker symbol and stock name for the given stock
    row['ticker_symbol'] = stk[0]
    row['stock_name'] = stk[1]
    row['sector'] = stk[2]
    row['industry'] = stk[3]
    row['hq_location'] = stk[4]
    return row

# get all rows of data for all stocks on the S&P 500
def extract_sp500() -> pd.DataFrame:
    stocks = get_sp500()

    # capture just the list of s&p 500 ticker symbols so we can pass it into another function later
    tickers = [stock[0].replace('.','-')for stock in stocks]

    # call the api for each stock found
    l = len(stocks)
    then = datetime.now()
    print(str(len(stocks))+ " stocks detected.")
    print("API call batch initiated at "+then.strftime('%Y-%m-%d %H:%M:%S')+'\n')
    with tqdm(total=l, position=0, leave=True) as load_bar:
        for index in tqdm(range(l), position=0, leave=True):
            r = get_row(api_headers, api_params['url'], stocks[index])
            stocks[index] = r
            load_bar.update()
    load_bar.close()
    # print total elapsed time and an estimate of time per row logged
    now = datetime.now()
    print("\n\nTime elapsed: " +str(now-then).split('.')[0])
    print("(Approximately "+str(round((now-then).total_seconds()/l, 2))+ " seconds for every row logged)")

    stocks = pd.DataFrame(stocks)
    return stocks[['call_at', 'ticker_symbol', 'stock_name', 'sector', 'industry', 'hq_location',
                  'total_vol', 'price', 'change_point', 'change_percentage']], tickers

# load data collected from extract_sp500() to bigquery instance
def load_to_bigquery(params:dict, df: pd.DataFrame=None, mode: str='append', from_file: str=None):
    # assert whether the dataframe is being passed in direct or if the user is indicating a path to read from
    if df is None and from_file is None:
        print("Data must be supplied either as a Pandas DataFrame in `df` or as a filepath to a CSV in `from_file`.")
        return None
    if df is None and from_file is not None:
        if not isinstance(from_file, str):
            print('`from_file` must be a string')
            return None
        try:
            df = pd.read_csv(from_file)
        except:
            print("Filepath provided for DataFrame is invalid.")
            return None
    print('\n'+str(len(df))+' rows of stock data collected. Loading to BigQuery instance...')
    gcp_creds = service_account.Credentials.from_service_account_file(
        params['keypath'],
        scopes = [params['scope_url']]
        )
    client = bigquery.Client(credentials=gcp_creds, project=params['project_id'])

    # control write_disposition of the BigQuery job-- print an error if the argument is written incorrectly
    wd = ''
    if mode == 'append':
        wd += 'WRITE_APPEND'
    if mode == 'overwrite':
        wd += 'WRITE_TRUNCATE'
    if mode != 'append' and mode != 'overwrite':
        print("Error. Only accepted arguments for `mode` are 'append' or 'overwrite'")
        return None
    
    job_config = bigquery.LoadJobConfig(write_disposition=wd)
    job = client.load_table_from_dataframe(df, params['table_id'], job_config=job_config)
    job.result()

    print("\nLoad to BigQuery completed at "+datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

def get_last_updated_datetime(params):
    gcp_creds = service_account.Credentials.from_service_account_file(
        params['keypath'],
        scopes = [params['scope_url']]
        )
    client = bigquery.Client(project=params['project_id'], credentials=gcp_creds)
    dataset_ref = client.dataset(params['dataset_id'])
    table_ref = dataset_ref.table(params['table_name'])
    table = client.get_table(table_ref)
    last_updated = table.modified
    return last_updated.replace(tzinfo=timezone.utc).astimezone(tz=None)

# get all stock info for companies on S&P 500, and load resulting DataFrame to BigQuery
def run_stonks():
    # extract and load up-to-date stock prices from API
    df, tickers = extract_sp500()
    load_to_bigquery(sp500_params, df)
    # check if market cap data has already been uploaded today
    # if not, write the data locally, then read it and upload
    # only update market cap data once per day
    last_updated = get_last_updated_datetime(mc_params)
    if last_updated.date() != datetime.today():
        market_cap_datafile = cache_market_caps(tickers)
        load_to_bigquery(mc_params, mode = 'overwrite', from_file=market_cap_datafile)



## DRIVER


time_window_end = datetime.now().replace(hour=16, minute=30, second=0, microsecond=0)

# don't run script after markets close for that day
if datetime.now()< time_window_end:
    run_stonks()