import pandas as pd
import talib
import json
import os


class CandlePatternRecognizer:
    def __init__(self, input_directory, output_directory):
        self.input_directory = input_directory
        self.output_directory = output_directory
        os.makedirs(self.output_directory, exist_ok=True)

        self.single_candle_patterns = {
            'Engulfing': talib.CDLENGULFING,
            'Hammer': talib.CDLHAMMER,
            'InvertedHammer': talib.CDLINVERTEDHAMMER,
            'ShootingStar': talib.CDLSHOOTINGSTAR,
            'Doji': talib.CDLDOJI,
            'DragonflyDoji': talib.CDLDRAGONFLYDOJI,
            'GravestoneDoji': talib.CDLGRAVESTONEDOJI,
            'PiercingLine': talib.CDLPIERCING,
            'DarkCloudCover': talib.CDLDARKCLOUDCOVER,
            'SpinningTop': talib.CDLSPINNINGTOP,
            'Marubozu': talib.CDLMARUBOZU,
            'AbandonedBaby': talib.CDLABANDONEDBABY,
            'CounterAttack': talib.CDLCOUNTERATTACK,
            'HangingMan': talib.CDLHANGINGMAN,
        }

    def list_files_in_directory(self):
        return [os.path.join(dirpath, filename)
                for dirpath, _, filenames in os.walk(self.input_directory)
                for filename in filenames]

    def recognize_candle_patterns(self, csv_file_path):
        base_file_name = os.path.basename(csv_file_path)
        df = pd.read_csv(csv_file_path)

        # Preprocess data
        df.rename(columns=lambda x: x.capitalize(), inplace=True)
        df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y')
        df.set_index('Date', inplace=True)

        # Detect patterns
        pattern_results = {name: func(df['Open'], df['High'], df['Low'], df['Close'])
                           for name, func in self.single_candle_patterns.items()}
        pattern_df = pd.DataFrame(pattern_results)
        df['Patterns'] = pattern_df.apply(lambda row: [name for name, val in row.items() if val != 0], axis=1)

        # Prepare and save CSV
        df.reset_index(inplace=True)
        df['Date'] = df['Date'].dt.date
        df['Patterns'] = df['Patterns'].apply(json.dumps)
        output_path = os.path.join(self.output_directory, base_file_name)
        df[['Symbol', 'Series', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Patterns']].to_csv(output_path, index=False)
        print(f"Data saved to {output_path}")


    def process_all_files(self):
        for file_path in self.list_files_in_directory():
            self.recognize_candle_patterns(file_path)


# Example usage
input_dir = r"D:\filter_datta"
output_dir = r"D:\image"

recognizer = CandlePatternRecognizer(input_dir, output_dir)
recognizer.process_all_files()