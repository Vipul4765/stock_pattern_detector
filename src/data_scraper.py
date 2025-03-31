import os
import requests
import pandas as pd
from io import StringIO
from next_date import increment_date
import warnings
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

    def download_csv_for_date(self, curr_date):
        """
        Download CSV content for a specific date.

        :param curr_date: Date in 'DDMMYYYY' format.
        :return: StringIO object containing CSV content, or None if download fails.
        """
        url = f'https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{curr_date}.csv'
        try:
            # Set headers to mimic a browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
            }
            # Send a GET request to download the file
            response = requests.get(url, headers=headers, stream=True)
            # Check if the request was successful
            if response.status_code == 200:
                # Decode the content and load it into a StringIO object
                csv_content = response.content.decode('utf-8')  # Decode bytes to string
                csv_file_like = StringIO(csv_content)  # Create a file-like object
                print(f"Successfully downloaded CSV content for date {curr_date}.")
                return csv_file_like  # Return the file-like object
            else:
                print(f"Failed to download for date {curr_date}. Status code: {response.status_code}")
                return None
        except Exception as e:
            print(f"Error downloading for date {curr_date}: {e}")
            return None

    def read_csv_to_dataframe(self, csv_file_like):
        """
        Read CSV content into a Pandas DataFrame.

        :param csv_file_like: StringIO object containing CSV content.
        :return: Pandas DataFrame, or None if reading fails.
        """
        try:
            df = pd.read_csv(csv_file_like)  # Read the file-like object into a DataFrame
            print("Successfully loaded CSV content into DataFrame.")
            return df
        except Exception as e:
            print(f"Error reading CSV content: {e}")
            return None

    def process_dates(self, output_dir):
        """
        Process all dates in the range [start_date, end_date].
        Downloads CSVs, loads them into DataFrames, and combines them.
        """
        current_date = self.start_date
        flag = False

        while not flag:
            if current_date == self.end_date:
                flag = True
            csv_file_like = self.download_csv_for_date(current_date)
            if csv_file_like:
                df = self.read_csv_to_dataframe(csv_file_like)
                if df is not None:
                    df=self.process_stock_data(df)
                    self.split_data(df, output_dir)
            current_date = increment_date(current_date)


    def process_stock_data(self, df):
        """
        This function processes stock data from a CSV file.

        Parameters:
        file_path (str): The path to the CSV file containing stock data.

        Returns:
        pd.DataFrame: A DataFrame containing the filtered and formatted stock data.
        """
        # Strip any leading or trailing spaces from column names
        df.columns = df.columns.str.strip()

        # Strip leading and trailing spaces from all string columns
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip()

        # Filter the DataFrame to include only rows where 'SERIES' is 'EQ'
        df = df[df['SERIES'] == 'EQ']

        # Drop unnecessary columns
        df.drop(columns=['PREV_CLOSE', 'LAST_PRICE', 'AVG_PRICE', 'TURNOVER_LACS', 'NO_OF_TRADES', 'DELIV_QTY', 'DELIV_PER'], inplace=True)

        # Rename columns for clarity
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

        # Format the dates to 'DD-MM-YYYY'
        df['Date'] = df['Date'].dt.strftime('%d-%m-%Y')

        return df

    def split_data(self,df, output_dir):

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Group by 'Symbol'
        symbol_wise_group = df.groupby('Symbol')

        for symbol, group in symbol_wise_group:
            # Define the file path for the symbol
            file_name = f"{symbol}.csv"
            file_path = os.path.join(output_dir, str(file_name.lower()))

            # Check if file exists
            if os.path.exists(file_path):
                # Append data to existing file
                group.to_csv(file_path, mode='a', header=False, index=False)
            else:
                # Create new file and write data
                group.to_csv(file_path, mode='w', header=True, index=False)

        print("Data has been split and saved to individual files.")


# Example Usage
if __name__ == "__main__":
    # Initialize the downloader with the desired date range
    downloader = StockDataDownloader(start_date="01012024", end_date="31032025")
    output_dir = r"D:\filter_datta"

    # Process all dates in the range
    downloader.process_dates(output_dir)