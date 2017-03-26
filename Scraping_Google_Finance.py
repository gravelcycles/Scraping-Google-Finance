
import pandas as pd
import urllib2
import datetime as dt
import csv
import os as os

path = "/Users/David/Documents/Market_Data_Main/"

# List of previous files by stock
stockFiles = os.listdir(path + "Stock_Data")
stockFiles = set(stockFiles[1:])  # Ignore .DStore file

# List of previous days
stock_by_day_files = os.listdir(path + "Stocks_By_Date")
stock_by_day_files = set(stock_by_day_files[1:])

class NoStockDataError(Exception):
    pass

def get_google_data(symbol, period, window):
    """
    Function for retrieving data from google.com/finance and then formatting the dataframe

    :param symbol: stock ticker
    :param period: frequency of stock quotes. Minimum: 60 seconds
    :param window: number of days requested
    :return: dataframe with formatted stock quotes
    """
    url_root = 'http://www.google.com/finance/getprices?i='
    url_root += str(period) + '&p=' + str(window)
    url_root += 'd&f=d,o,h,l,c,v&df=cpct&q=' + symbol + '&x=NASDAQ'
    response = urllib2.urlopen(url_root)
    data = response.read().split('\n')

    # actual data starts at index = 7
    # first line contains full timestamp,
    # every other line is offset of period from timestamp
    parsed_data = []
    anchor_stamp = ''
    end = len(data)
    if end < 10:
        raise NoStockDataError
    for i in range(7, end):
        cdata = data[i].split(',')
        cts = 0
        if 'a' in cdata[0]:
            # first one record anchor timestamp
            anchor_stamp = cdata[0].replace('a', '')
            cts = int(anchor_stamp)
        elif 'TIMEZONE_OFFSET' in cdata[0] or cdata[0] is '':
            continue
        else:
            coffset = int(cdata[0])
            cts = int(anchor_stamp) + (coffset * period)

        parsed_data.append([symbol, str(dt.datetime.fromtimestamp(cts)),
                           float(cts), float(cdata[1]),
                           float(cdata[2]), float(cdata[3]),
                           float(cdata[4]),  float(cdata[5])])

    df = pd.DataFrame(parsed_data)
    df.columns = ['ticker', 'timestamp', 'unix_time', 'open', 'high', 'low', 'close', 'volume']

    return df


files_opened = set()


def add_company_to_day(stock_data):
    stock_date_stamp = stock_data['timestamp'].apply(lambda x: x.split(" ")[0])
    unique_dates = stock_date_stamp.unique()
    for date in unique_dates:
        date_csv = date + '.csv'

        temp = stock_data[stock_date_stamp == date]

        with open(path + "Stocks_By_Date/" + date_csv, 'a') as file:
            if date_csv in stock_by_day_files:
                temp.to_csv(file, header=False, index=False)
            else:
                temp.to_csv(f, header=True, index=False)

        files_opened.add(date_csv)
        stock_by_day_files.add(date_csv)


def add_company_to_stock(new_stock_data, company_name):
    company_csv = company_name + '.csv'
    if company_csv in stockFiles:
        old_stock_data = pd.read_csv(path + "Stock_Data/%s" % company_csv)
        first_entry = old_stock_data['timestamp'].iloc(-1)

        if first_entry not in new_stock_data['timestamp']:
            old_stock_data = old_stock_data.append(new_stock_data)
        else:
            init_index = new_stock_data['timestamp'].index.get_loc(first_entry)
            old_stock_data = old_stock_data.append(new_stock_data[init_index+1:])

        old_stock_data.to_csv(path + "Stock_Data/%s" % company_csv, index=False)
    else:
        new_stock_data.to_csv(path + "Stock_Data/{}".format(company_csv),
                              index=False)
        stockFiles.add(company_csv)


companylist = None
with open(path + 'companylist.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='|')
    companylist = list(reader)
    companylist = companylist[0]
    print companylist

start = 0
companylist = companylist[start:1000]

increment = start
missed = []
for company in companylist:
    try:
        company = str(company.strip('"'))
        company_data = get_google_data(company, 60, 20)

        add_company_to_day(company_data)
        add_company_to_stock(company_data, company)

        increment += 1
    except urllib2.URLError, e:
        checksLogger.error('URLError = ' + str(e.reason))
    except NoStockDataError, e:
        increment += 1
        print company
        missed.append(company)


for file in files_opened:
    current = pd.read_csv(path + "Stocks_By_Date/%s" % file)
    current.drop_duplicates(inplace=True)
    current.to_csv(path + "Stocks_By_Date/%s" % file, index=False)

# sys.exit('hello')
