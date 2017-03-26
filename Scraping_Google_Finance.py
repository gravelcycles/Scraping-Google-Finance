import pandas as pd
import urllib2
import datetime as dt
import os as os

start_stock = 0

path = "/Users/David/Documents/Market_Data_Main/"

# List of previous files by stock
stockFiles = os.listdir(path + "Stock_Data")
stockFiles = set(stockFiles[1:])  # Ignore .DStore file

# List of previous days
stock_by_day_files = os.listdir(path + "Stocks_By_Date")
stock_by_day_files = set(stock_by_day_files[1:])


class NoStockDataError(Exception):
    pass


def get_google_data(symbol, frequency=60, window=20, exchange='NASDAQ'):
    """
    Function for retrieving data from google.com/finance and then formatting the dataframe

    :param symbol: stock ticker
    :param frequency: frequency of stock quotes. Minimum: 60 seconds
    :param window: number of days requested
    :param exchange: Stock Exchange
    :return: dataframe with formatted stock quotes
    """
    url_root = 'http://www.google.com/finance/getprices?i='
    url_root += str(frequency) + '&p=' + str(window)
    url_root += 'd&f=d,o,h,l,c,v&df=cpct&q=' + symbol + '&x=' + exchange
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
            cts = int(anchor_stamp) + (coffset * frequency)

        parsed_data.append([symbol, str(dt.datetime.fromtimestamp(cts)),
                           float(cts), float(cdata[1]),
                           float(cdata[2]), float(cdata[3]),
                           float(cdata[4]),  float(cdata[5])])

    df = pd.DataFrame(parsed_data)
    df.columns = ['company', 'timestamp', 'unix_time', 'open', 'high', 'low', 'close', 'volume']

    return df

# Keeps track of files from Stocks_By_Date folder that have been used in order
# to clean up duplicates at the end
files_opened = set()


def add_company_to_day(stock_data):
    """
    Splits a company's stock data by day and writes to appropriate day file

    :param stock_data:
    :return: None
    """

    stock_date_stamp = stock_data['timestamp'].apply(lambda x: x.split(" ")[0])
    unique_dates = stock_date_stamp.unique()

    for date in unique_dates:
        date_csv = date + '.csv'

        # Creates dataframe of date's daya
        todays_data = stock_data[stock_date_stamp == date]

        # If the file already exists, appends data. Otherwise, creates new file
        if date_csv in stock_by_day_files:
            todays_data.to_csv(path + "Stocks_By_Date/" + date_csv, header=False, index=False)
        else:
            todays_data.to_csv(path + "Stocks_By_Date/" + date_csv, header=True, index=False)

        files_opened.add(date_csv)
        stock_by_day_files.add(date_csv)


def add_company_to_stock(new_stock_data, company_name):
    """
    Adds a company's stock data to an already existing file of data or a new file.

    :param new_stock_data:
    :param company_name:
    :return: None
    """
    company_csv = company_name + '.csv'
    if company_csv in stockFiles:

        # Finds where the new data overlaps with the old data
        old_stock_data = pd.read_csv(path + "Stock_Data/%s" % company_csv)
        first_entry = old_stock_data['timestamp'].iloc(-1)

        # Only appends the non-duplicate data
        if first_entry not in new_stock_data['timestamp']:
            old_stock_data = old_stock_data.append(new_stock_data)
        else:
            init_index = new_stock_data['timestamp'].index.get_loc(first_entry)
            old_stock_data = old_stock_data.append(new_stock_data[init_index+1:])

        old_stock_data.to_csv(path + "Stock_Data/%s" % company_csv, index=False)

    # In the case that the stock does not exist
    else:
        new_stock_data.to_csv(path + "Stock_Data/{}".format(company_csv),
                              index=False)
        stockFiles.add(company_csv)


# Gets list of NASDAQ stocks
companylist = None
companylist = list(pd.read_csv(path+ 'companylist.csv'))
print companylist


# Sets up iteration over <1001 stocks in order to prevent Google timeout
if start_stock + 1000 < len(companylist):
    companylist = companylist[start_stock:1000]
elif start_stock < len(companylist):
    companylist = companylist[start_stock:]
else:
    raise IndexError


increment = start_stock
missed = []
for company in companylist:
    try:
        increment += 1
        company = str(company.strip('"'))
        company_data = get_google_data(company)

        add_company_to_day(company_data)
        add_company_to_stock(company_data, company)


    except urllib2.URLError, e:
        checksLogger.error('URLError = ' + str(e.reason))

    # In the case that Google does not have the stock
    except NoStockDataError, e:
        print increment
        print company
        missed.append(company)

# Remove Duplicates for opened Stock_By_Date files
for file in files_opened:
    current = pd.read_csv(path + "Stocks_By_Date/%s" % file)
    current.drop_duplicates(inplace=True)
    current.to_csv(path + "Stocks_By_Date/%s" % file, index=False)
