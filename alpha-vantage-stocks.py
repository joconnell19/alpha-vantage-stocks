"""alpha-vantage-stocks.py: Uses AlphaVantage API to get the following stock data:

1. Company name
2. Headquarter state
3. Sector the company belongs to
4. Stock price at the nearest fiscal year end (P1)
5. Stock price at the previous fiscal year end (P0)
6. Sales (or revenues) of the most recent fiscal year (S1)
7. Sales (or revenues) of the prior fiscal year (S0)
8. Total shares outstanding
9. beta

Author:         Jamie O'Connell
Email:          joconnell19@georgefox.edu
Version:        1.0.2
Last Updated:   2023/03/02
"""


import csv
import alpha_vantage
import requests
import os
import pandas as pd
import datetime
from alpha_vantage.timeseries import TimeSeries

key = 'INSERT ALPHA VANTAGE API KEY HERE'
ts = TimeSeries(key=key)
companies_to_extrapolate = 100
last_year = '2022'
# stock_index = 'GSPC'

def get_companies(filename, stock_index):
    """
    Gets all companies from a given stock index and writes them to file of filename
    :param filename: file to write companies to
    :param stock_index: stock index to get companies from
    """
    # Get json object with the intraday data and another with the call's metadata
    url = 'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey='+key
    df = pd.read_csv(url)
    # print(df.values)

    file = open(filename, 'a')

    csv_writer = csv.writer(file)
    csv_writer.writerow(df)
    for company in df.values:
        if company[2] == stock_index:
            csv_writer.writerow(company)


def get_symbols_from_companies(filename):
    """

    :param filename:
    :return:
    """
    df = pd.read_csv(filename)
    symbols = []

    for line in df.values:
        symbol = line[0]
        symbols.append(symbol)

    return symbols


def get_info_for_companies(symbols, filename):
    """
    Iterate through list of symbols to get info and write to filename (comma separated)

    :return:
    """
    data_file = open(filename, 'a')
    header = ','.join(['symbol', 'name', 'state', 'sector', 'p1', 'p0', 's1', 's0', 'shares outstanding',
              'full time employee count', 'beta', '\n'])

    data_file.write(header)
    data_file.close()
    output = []

    for symbol in symbols:
        # get INCOME STATEMENT (fiscal end date, s1, s0)
        nearest_fiscal_date_end, s1, s0 = get_income_statement(symbol)

        # if fiscal date year is 2022
        if nearest_fiscal_date_end[0:4] == last_year:
            # get COMPANY OVERVIEW (returns: name. state, sector, shares outstanding, beta)
            name, state, sector, shares, beta = get_company_overview(symbol)

            if state != '':
                # get INTRADAY (p1, p0)
                p1, p0 = get_intraday_ext(symbol, nearest_fiscal_date_end)

                # append to output
                if p1 != 0 and p0 != 0:
                    output.append({'symbol': symbol, 'name': name, 'state': state, 'sector': sector, 'p1': p1, 'p0': p0,
                                   's1': s1, 's0': s0, 'shares outstanding': shares, 'full time employee count': '0',
                                   'beta': beta, 'end': '\n'})
                    print('\t SUCCESS', len(output), output[-1])
                    file = open('data.txt', 'a')
                    file.write(','.join(output[-1].values()))
                    file.close()


def get_income_statement(symbol):
    """
    Gets nearest fiscal date end, most recent fiscal end sales (s1), and previous fiscal end sales (s0)
    for the given company's symbol

    :param symbol: ticker symbol for company
    :return: nearest fiscal date end, most recent fiscal end sales (s1), and previous fiscal end sales (s0)
    """
    try:
        url = f'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey={key}'
        r = requests.get(url)
        data = r.json()

        nearest_fiscal_date_end = data['annualReports'][0]['fiscalDateEnding']
        s1 = data['annualReports'][0]['totalRevenue']
        s0 = data['annualReports'][1]['totalRevenue']
    except:
        print('\t income statement error')
        nearest_fiscal_date_end = '0001-01-01'
        s1 = 0
        s0 = 0
    return nearest_fiscal_date_end, s1, s0


def get_company_overview(symbol):
    """
    Gets state, sector, shares outstanding, beta for the given company's symbol

    :param symbol: ticker symbol for company
    :return: state, sector, shares outstanding, beta
    """
    try:
        url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={key}'
        r = requests.get(url)
        data = r.json()

        name = data['Name']
        state = data['Address'][-6:-4]
        sector = data['Sector']
        shares = data['SharesOutstanding']
        beta = data['Beta']
    except:
        print('\t company overview error')
        name = ''
        state = ''
        sector = ''
        shares = 0
        beta = 0

    return name, state, sector, shares, beta


def get_intraday_ext(symbol, nearest_fiscal_date_end):
    """
    Gets price at end of nearest fiscal year (p1) and price at end of prev fiscal year (p0)
    for the given company's symbol

    :param symbol: ticker symbol for company
    :param nearest_fiscal_date_end: date in which the given company's nearest fiscal year ended
    :return: price at end of nearest fiscal year (p1) and price at end of prev fiscal year (p0)
    """
    try:
        today = datetime.date.today()
        p1_date = datetime.date(year=int(nearest_fiscal_date_end[0:4]), month=int(nearest_fiscal_date_end[5:7]),
                                day=int(nearest_fiscal_date_end[8:]))

        # compute slice based on today - nearest_fiscal_date_end
        months_from_today = int((today - p1_date).days / 30) + 1

        p1_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=' \
                  f'{symbol}&interval=60min&slice=year1month{months_from_today}&apikey={key}'
        p0_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=' \
                  f'{symbol}&interval=60min&slice=year2month{months_from_today}&apikey={key}'

        p1 = 0
        p0 = 0

        with requests.Session() as s:
            p1_download = s.get(p1_url)
            decoded_content = p1_download.content.decode('utf-8')
            cr = csv.DictReader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)

            for row in my_list:
                if row['time'][0:10] == nearest_fiscal_date_end:
                    p1 = row['close']

        with requests.Session() as s:
            p0_download = s.get(p0_url)
            decoded_content = p0_download.content.decode('utf-8')
            cr = csv.DictReader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
            for row in my_list:
                if row['time'][0:10] == '2021' + nearest_fiscal_date_end[4:]:
                    p0 = row['close']
    except:
        print('\t intraday ext error')
        p1 = 0
        p0 = 0

    return p1, p0

