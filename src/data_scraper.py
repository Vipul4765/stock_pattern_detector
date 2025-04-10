import os
import requests
import pandas as pd
from io import StringIO
from next_date import increment_date
import warnings
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    filename='stock_downloader.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

warnings.filterwarnings("ignore")


class StockDataDownloader:
    def __init__(self, start_date, end_date, download_dir="D:\\stock_data_csv"):
        """
        Initialize the StockDataDownloader class.

        :param start_date: Start date in 'DDMMYYYY' format.
        :param end_date: End date in 'DDMMYYYY' format.
        :param download_dir: Directory to save downloaded files (optional).
        """
        self.start_date = start_date
        self.end_date = end_date
        self.download_dir = download_dir
        self.all_dataframes = []

    def is_weekend(self, date_str):
        """
        Check if a given date is Saturday or Sunday.

        :param date_str: Date in 'DDMMYYYY' format.
        :return: True if weekend, False otherwise.
        """
        try:
            date_obj = datetime.strptime(date_str, "%d%m%Y")
            return date_obj.weekday() >= 5
        except Exception as e:
            logging.error(f"Error parsing date {date_str}: {e}")
            return False

    def download_csv_for_date(self, curr_date):
        """
        Download CSV content for a specific date.

        :param curr_date: Date in 'DDMMYYYY' format.
        :return: StringIO object containing CSV content, or None if download fails.
        """
        url = f'https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{curr_date}.csv'
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
            }
            response = requests.get(url, headers=headers, stream=True)
            if response.status_code == 200:
                csv_content = response.content.decode('utf-8')
                csv_file_like = StringIO(csv_content)
                logging.info(f"✅ Downloaded CSV for {curr_date}")
                return csv_file_like
            else:
                logging.warning(f"❌ Failed to download for {curr_date}. Status: {response.status_code}")
                return None
        except Exception as e:
            logging.error(f"⚠️ Error downloading for {curr_date}: {e}")
            return None

    def read_csv_to_dataframe(self, csv_file_like):
        """
        Read CSV content into a Pandas DataFrame.

        :param csv_file_like: StringIO object containing CSV content.
        :return: Pandas DataFrame, or None if reading fails.
        """
        try:
            df = pd.read_csv(csv_file_like)
            logging.info("✅ CSV loaded into DataFrame")
            return df
        except Exception as e:
            logging.error(f"⚠️ Error reading CSV: {e}")
            return None

    def process_dates(self, output_dir):
        """
        Process all dates in the range [start_date, end_date].
        Skips Saturdays and Sundays.
        """
        current_date = self.start_date
        while True:
            if current_date == self.end_date:
                break

            if self.is_weekend(current_date):
                logging.info(f"⏭️ Skipping weekend: {current_date}")
                current_date = increment_date(current_date)
                continue

            csv_file_like = self.download_csv_for_date(current_date)
            if csv_file_like:
                df = self.read_csv_to_dataframe(csv_file_like)
                if df is not None:
                    df = self.process_stock_data(df)
                    self.split_data(df, output_dir)

            current_date = increment_date(current_date)

    def process_stock_data(self, df):
        """
        Process stock data from a DataFrame.

        :param df: Raw stock data DataFrame.
        :return: Processed DataFrame.
        """
        df.columns = df.columns.str.strip()
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip()

        df = df[df['SERIES'] == 'EQ']
        df.drop(columns=['PREV_CLOSE', 'LAST_PRICE', 'AVG_PRICE', 'TURNOVER_LACS',
                         'NO_OF_TRADES', 'DELIV_QTY', 'DELIV_PER'], inplace=True)

        df.rename(columns={
            'SYMBOL': 'Symbol',
            'SERIES': 'Series',
            'DATE1': 'Date',
            'OPEN_PRICE': 'Open',
            'HIGH_PRICE': 'High',
            'LOW_PRICE': 'Low',
            'CLOSE_PRICE': 'Close',
            'TTL_TRD_QNTY': 'Volume',
        }, inplace=True)

        df['Date'] = pd.to_datetime(df['Date'], format='%d-%b-%Y')
        df['Date'] = df['Date'].dt.strftime('%d-%m-%Y')

        return df

    def split_data(self, df, output_dir):
        """
        Split and save data symbol-wise.

        :param df: Processed stock DataFrame.
        :param output_dir: Directory to save the split CSVs.
        """
        os.makedirs(output_dir, exist_ok=True)
        symbol_wise_group = df.groupby('Symbol')

        for symbol, group in symbol_wise_group:
            file_name = f"{symbol}.csv".lower()
            file_path = os.path.join(output_dir, file_name)

            try:
                if os.path.exists(file_path):
                    group.to_csv(file_path, mode='a', header=False, index=False)
                else:
                    group.to_csv(file_path, mode='w', header=True, index=False)
            except Exception as e:
                logging.error(f"⚠️ Failed saving data for {symbol}: {e}")

        logging.info("✅ Data saved symbol-wise")


# Example usage
if __name__ == "__main__":
    downloader = StockDataDownloader(start_date="01012024", end_date="05042025")
    output_dir = r"D:\filter_datta"
    downloader.process_dates(output_dir)
