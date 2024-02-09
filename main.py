import os
import socket
import zipfile
import pandas as pd
import urllib.request
from datetime import datetime, timedelta


def download_file(url, output_folder):
    filename = os.path.basename(url)
    output_path = os.path.join(output_folder, filename)
    urllib.request.urlretrieve(url, output_path)
    return output_path


def extract_files(zip_file, output_folder):
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(output_folder)


def rename_files(directory):
    files = os.listdir(directory)
    renamed_files = []

    for file in files:
        if file.endswith("bhav.csv"):
            date = file[2:11]
            new_name = date + ".csv"
            new_name = new_name.replace("cm", "").replace("bhav", "")
            date_obj = datetime.strptime(date, "%d%b%Y")
            new_name = date_obj.strftime("%Y-%m-%d") + "-NSE-EQ.csv"

            old_path = os.path.join(directory, file)
            new_path = os.path.join(directory, new_name)
            os.rename(old_path, new_path)
            renamed_files.append((file, new_name))
        else:
            print(
                f"File '{file}' does not end with 'bhav.csv' and was not renamed.")


def setup():
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    else:
        filelist = [f for f in os.listdir(output_folder) if (
            f.endswith(".csv") or f.endswith(".zip"))]
        for f in filelist:
            os.remove(os.path.join(output_folder, f))

    delta = end_date - start_date
    date_range = []
    for i in range(delta.days + 1):
        date = start_date + timedelta(days=i)
        if date.weekday() < 5:
            date_range.append(date)
    socket.setdefaulttimeout(1)

    downloaded_files = []  # Downloads files
    for date in date_range:
        url = "https://archives.nseindia.com/content/historical/EQUITIES/{}/{}".format(
            date.strftime("%Y"), date.strftime("%b").upper())
        date_str = date.strftime("%d%b%Y").upper()
        filename = "cm{}bhav.csv.zip".format(date_str)
        url = "{}/{}".format(url, filename)

        try:
            downloaded_file = download_file(url, output_folder)
            downloaded_files.append(downloaded_file)
        except Exception as e:
            print("Error downloading file, Probable holiday:", filename)
            print(e)
    # print(downloaded_files[-1])

    for file in downloaded_files:
        try:
            extract_files(file, output_folder)
        except Exception as e:
            print("Error extracting file:", file)
            print(e)

    for file in downloaded_files:
        os.remove(file)

    rename_files(output_folder)


##############
def main():
    folder = os.getcwd()
    pd.set_option('display.max_columns', 500)

    files_list = [file for file in os.listdir(
        f'{folder}\\') if file.endswith(".csv")]

    closingFilter = {}
    avgRangeFilter = {}
    avgVolumeFilter = {}
    leastVolumeFilter = {}

    del_columns = ['LAST', 'OPEN', 'PREVCLOSE', 'TOTTRDVAL',
                   'TIMESTAMP', 'TOTALTRADES', 'ISIN', 'Unnamed: 13']

    for index in range(len(files_list)):
        df = pd.read_csv(files_list[index])
        for col in del_columns:
            df.pop(item=col)
        for row_index in range(df.shape[0]):
            if df.at[row_index, 'SYMBOL'] in closingFilter.keys():
                if closingFilter[df.at[row_index, 'SYMBOL']] < df.at[row_index, 'CLOSE']:
                    closingFilter[df.at[row_index, 'SYMBOL']
                                  ] = df.at[row_index, 'CLOSE']
            else:
                if (df.at[row_index, 'CLOSE'] >= 0.75 * df.at[row_index, 'CLOSE']):
                    closingFilter[df.at[row_index, 'SYMBOL']
                                  ] = df.at[row_index, 'CLOSE']

    df = pd.read_csv(files_list[-1])
    for col in del_columns:
        df.pop(item=col)
    df = df.query('SERIES == "EQ"')
    df = df.query('CLOSE >= .75* SYMBOL.map(@closingFilter)')

    print("\nQueried the dataframe (CURRENT CLOSE > 0.75 * 52WH CLOSE)")
    df.to_excel('REPORTS\\FIRST REPORT.xlsx')

    print(df.shape)
    # PHASE 1 COMPLETED--------------------------------------------------------------------------------------------

    for index in range(len(files_list[-2:-21:-1])):
        avgDF = pd.read_csv(files_list[index])
        avgDF.insert(len(avgDF.columns), 'RANGE',
                     value=(avgDF['HIGH'] - avgDF['LOW']))
        for col in del_columns:
            avgDF.pop(item=col)
        for row_index in range(avgDF.shape[0]):
            symbol = avgDF.at[row_index, 'SYMBOL']
            if symbol in set(df['SYMBOL']):
                if symbol in avgVolumeFilter:
                    avgRangeFilter[symbol] += avgDF.at[row_index, 'RANGE']
                else:
                    avgRangeFilter[symbol] = avgDF.at[row_index, 'RANGE']
    else:
        for symbol in avgRangeFilter:
            avgRangeFilter[symbol] /= 20
    df.insert(len(df.columns), 'RANGE', value=(df['HIGH'] - df['LOW']))
    df.insert(len(df.columns), 'AVG_RANGE_20DAYS',
              value=df['SYMBOL'].map(avgRangeFilter))
    df = df.query('RANGE < 0.5*SYMBOL.map(@avgRangeFilter)')
    print("\nQueried the dataframe (CURRENT RANGE < 0.5 * AVERAGE RANGE OVER PAST 20 DAYS)")
    df.to_excel('REPORTS\\SECOND REPORT.xlsx')
    print(df.shape)
    # PHASE 2 COMPLETED--------------------------------------------------------------------------------------------

    for index in range(len(files_list[-2:-21:-1])):
        avgDF = pd.read_csv(files_list[index])
        for col in del_columns:
            avgDF.pop(item=col)
        for row_index in range(avgDF.shape[0]):
            symbol = avgDF.at[row_index, 'SYMBOL']
            if symbol in set(df['SYMBOL']):
                if symbol in avgVolumeFilter:
                    avgVolumeFilter[symbol] += avgDF.at[row_index, 'TOTTRDQTY']
                else:
                    avgVolumeFilter[symbol] = avgDF.at[row_index, 'TOTTRDQTY']
    else:
        for symbol in avgVolumeFilter:
            avgVolumeFilter[symbol] /= 20

    df.insert(len(df.columns), 'AVGVOL20',
              value=df['SYMBOL'].map(avgVolumeFilter))
    df = df.query('AVGVOL20 > 25000')

    print("\nQueried the dataframe (AVERAGE VOLUME OF 20 DAYS > 25000)")
    df.to_excel('REPORTS\\THIRD REPORT.xlsx')

    for index in range(len(files_list[-2:-21:-1])):
        avgDF = pd.read_csv(files_list[index])
        for col in del_columns:
            avgDF.pop(item=col)
        for row_index in range(avgDF.shape[0]):
            symbol = avgDF.at[row_index, 'SYMBOL']
            if symbol in set(df['SYMBOL']):
                if symbol not in leastVolumeFilter:
                    leastVolumeFilter[symbol] = avgDF.at[row_index,
                                                         'TOTTRDQTY']
                else:
                    if leastVolumeFilter[symbol] > avgDF.at[row_index, 'TOTTRDQTY']:
                        leastVolumeFilter[symbol] = avgDF.at[row_index,
                                                             'TOTTRDQTY']

    df.insert(len(df.columns)-1, 'LOWEST_VOLUME',
              value=df['SYMBOL'].map(leastVolumeFilter))
    df = df.query('TOTTRDQTY <= LOWEST_VOLUME')

    print("\nQueried the dataframe (CURRENT VOLUME <= LOWEST VOLUME IN THE PAST 20 DAYS)")
    print(df.shape)
    df.to_excel('REPORTS\\CURRENT REPORTS.xlsx')
    print("\nDataFrame is written to Excel File successfully.")


output_folder = os.getcwd()
directory = "REPORTS"
path = os.path.join(output_folder, directory)
if not os.path.exists(path):
    os.makedirs(path)
start_date = datetime.strptime(
    (datetime.now() - timedelta(weeks=52)).strftime('%Y-%m-%d'), '%Y-%m-%d')
end_date = datetime.strptime(
    (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d'), '%Y-%m-%d')

menu = '''
Choose option
DATA TO TRACKED OVER A PERIOD OF
1. LAST 12 MONTHS
2. LAST 3 MONTHS
3. LAST 1 MONTHS
4. PROCESS GIVEN DATA
5. EXIT'''
while True:
    print(menu)
    option = int(input())
    print()
    if option == 1:
        start_date = datetime.strptime(
            (datetime.now() - timedelta(weeks=52)).strftime('%Y-%m-%d'), '%Y-%m-%d')
        setup()
    elif option == 2:
        start_date = datetime.strptime(
            (datetime.now() - timedelta(weeks=12)).strftime('%Y-%m-%d'), '%Y-%m-%d')
        setup()
    elif option == 3:
        start_date = datetime.strptime(
            (datetime.now() - timedelta(weeks=4)).strftime('%Y-%m-%d'), '%Y-%m-%d')
        setup()
    elif option == 4:

        main()
    else:
        print("EXITING")
        break
