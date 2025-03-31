import os
import pandas as pd
import talib
import mplfinance as mpf


class DrawPatternImage:

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

    def compute_patterns(self, df):
        pattern_df = pd.DataFrame(index=df.index)
        for pattern_name, pattern_func in self.single_candle_patterns.items():
            pattern_values = pattern_func(
                df['Open'].values, df['High'].values,
                df['Low'].values, df['Close'].values
            )
            pattern_df[pattern_name] = pattern_values
        return pattern_df

    def _generate_candle_plot(self, df, base_name, pattern_df):
        image_path = os.path.join(self.output_directory, f"{os.path.splitext(base_name)[0]}.png")
        apds = []

        try:
            for pattern in self.single_candle_patterns:
                if pattern not in pattern_df.columns:
                    continue  # Skip if column doesn't exist

                series = pattern_df[pattern]
                bullish = series > 0
                bearish = series < 0

                # Bullish markers
                if bullish.any():
                    y_bull = df.loc[bullish, 'High'] * 1.005
                    x_bull = df.loc[bullish].index
                    apds.append(mpf.make_addplot(y_bull, type='scatter', markersize=100,
                                                marker='^', color='lime', x=x_bull))

                # Bearish markers
                if bearish.any():
                    y_bear = df.loc[bearish, 'Low'] * 0.995
                    x_bear = df.loc[bearish].index
                    apds.append(mpf.make_addplot(y_bear, type='scatter', markersize=100,
                                                marker='v', color='red', x=x_bear))

            if apds:
                mpf.plot(df, type='candle', addplot=apds, style='yahoo',
                         title=base_name, figsize=(16, 8),
                         savefig=dict(fname=image_path, dpi=100), volume=False)
                print(f"Chart saved to {image_path}")
            else:
                print(f"No patterns detected for {base_name}, skipping plot.")

        except Exception as e:
            print(f"Error generating chart for {base_name}: {e}")

    def process_all_files(self):
        for file_path in self.list_files_in_directory():
            try:
                # Read CSV file
                df = pd.read_csv(file_path, parse_dates=True, index_col=0)
                # Compute patterns
                pattern_df = self.compute_patterns(df)
                # Generate base name from file path
                base_name = os.path.basename(file_path)
                # Generate plot
                self._generate_candle_plot(df, base_name, pattern_df)
            except Exception as e:
                print(f"Error processing {file_path}: {e}")


# Example usage
input_csv_files = r"D:\image"
output_csv_files = r"D:\image_database"
obj = DrawPatternImage(input_csv_files, output_csv_files)
obj.process_all_files()