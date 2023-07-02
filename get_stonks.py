import bs4 as bs
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import pandas as pd
from re import sub, match
import requests
from tqdm import tqdm


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
gcp_params_env = os.environ.get('GCP_PARAMS')
gcp_params = {
        'keypath': gcp_params_env.split(';')[0],
        'project_id':gcp_params_env.split(';')[1],
        'dataset_id':gcp_params_env.split(';')[2],
        'table_name':gcp_params_env.split(';')[3],
        'table_id':gcp_params_env.split(';')[4],
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
                  'total_vol', 'price', 'change_point', 'change_percentage']]

# load data collected from extract_sp500() to bigquery instance
def load_to_bigquery(params:dict, df: pd.DataFrame):
    print('\n'+str(len(df))+' rows of stock data collected. Loading to BigQuery instance...')
    gcp_creds = service_account.Credentials.from_service_account_file(
        params['keypath'],
        scopes = [params['scope_url']]
        )
    client = bigquery.Client(credentials=gcp_creds, project=params['project_id'])
    job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND')
    job = client.load_table_from_dataframe(df, params['table_id'], job_config=job_config)
    job.result()
    # data = client.get_table(params['table_id'])
    print("\nLoad to BigQuery completed at "+datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

# driver -> get all stock info for companies on S&P 500, and load resulting DataFrame to BigQuery
def run_stonks():
    df = extract_sp500()
    load_to_bigquery(gcp_params, df)

run_stonks()