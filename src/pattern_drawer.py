import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
from matplotlib.patches import Ellipse
import json
import io
import base64
import os

# Function to list files in a directory
def list_files_in_directory(directory):
    full_paths = []
    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            full_paths.append(full_path)
    return full_paths

# Main function to annotate patterns in charts
def annotate_patterns_in_charts(input_directory=r"D:\image", output_csv="D:\database_final\output_with_images.csv", image_save_directory="D:\pattern_images"):
    all_files = list_files_in_directory(input_directory)

    if not os.path.exists(image_save_directory):
        os.makedirs(image_save_directory)  # Create the directory if it does not exist

    count = 0
    for input_csv in all_files:
        df = pd.read_csv(input_csv)

        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')

        # Function to safely load JSON from the 'Patterns' column
        def safe_json_loads(pattern):
            try:
                return json.loads(pattern)
            except (json.JSONDecodeError, TypeError):
                return []  # Return an empty list for invalid JSON

        df['Patterns'] = df['Patterns'].apply(safe_json_loads)
        df.sort_values(by='Date', inplace=True)
        df.set_index('Date', inplace=True)

        pattern_index = df[df['Patterns'].apply(lambda x: len(x) > 0)].index

        # Define colors for patterns
        pattern_colors = {
            'Engulfing': 'blue',
            'Hammer': 'lightgreen',
            'InvertedHammer': 'lightcoral',
            'ShootingStar': 'darkred',
            'Doji': 'green',
            'DragonflyDoji': 'cyan',
            'GravestoneDoji': 'orange',
            'PiercingLine': 'lime',
            'DarkCloudCover': 'darkorange',
            'SpinningTop': 'red',
            'Marubozu': 'purple',
            'AbandonedBaby': 'pink',
            'CounterAttack': 'brown',
            'HangingMan': 'gray',
        }

        market_colors = mpf.make_marketcolors(up='green', down='red', wick='inherit', edge='inherit')
        custom_style = mpf.make_mpf_style(marketcolors=market_colors)

        df['Pattern_Image'] = None  # Add an empty column for base64 images

        for idx in pattern_index:
            try:
                if idx >= df.index[0] and idx <= df.index[-1]:
                    start_idx = max(0, df.index.get_loc(idx) - 20)
                    end_idx = df.index.get_loc(idx) + 1

                    pattern_candles = df.iloc[start_idx:end_idx]
                    pattern_candles.index = pd.to_datetime(pattern_candles.index)

                    ohlc_data = pattern_candles[['Open', 'High', 'Low', 'Close']].astype(float)

                    fig, axlist = mpf.plot(
                        ohlc_data, type='candle', style=custom_style,
                        title=f"Patterns: {', '.join(df.loc[idx]['Patterns'])} on {idx.date()}",
                        returnfig=True
                    )

                    ax = axlist[0]
                    curr_candle = df.loc[idx]
                    curr_x = len(pattern_candles) - 1

                    curr_high = curr_candle['High']
                    curr_low = curr_candle['Low']
                    y_center = (curr_high + curr_low) / 2

                    for i, pattern in enumerate(curr_candle['Patterns']):
                        adjusted_y = y_center + i * 0.3 * (curr_high - curr_low)
                        color = pattern_colors.get(pattern, 'black')

                        oval = Ellipse((curr_x, adjusted_y), width=0.5, height=(curr_high - curr_low) * 0.5,
                                       color=color, fill=False, lw=2)
                        ax.add_patch(oval)

                        ax.annotate(pattern, xy=(curr_x, adjusted_y),
                                    xytext=(curr_x + 0.9, adjusted_y + 0.3 * (curr_high - curr_low)),
                                    arrowprops=dict(arrowstyle='->', color='black'),
                                    fontsize=10, color='black', ha='left', va='center')

                    textstr = f'Open: {curr_candle["Open"]:.2f}\nLow: {curr_candle["Low"]:.2f}\nClose: {curr_candle["Close"]:.2f}'
                    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
                    ax.text(curr_x + 0.1, curr_high + 2, textstr, fontsize=10,
                            verticalalignment='top', bbox=props, ha='right', va='bottom')

                    ax.plot(curr_x, curr_candle['Open'], 'go', markersize=6)
                    ax.plot(curr_x, curr_candle['Low'], 'bo', markersize=6)
                    ax.plot(curr_x, curr_candle['Close'], 'ro', markersize=6)

                    handles, labels = ax.get_legend_handles_labels()
                    unique_labels = dict(zip(labels, handles))

                    buf = io.BytesIO()
                    fig.savefig(buf, format='png')
                    plt.close(fig)
                    buf.seek(0)

                    img_base64 = base64.b64encode(buf.read()).decode('utf-8')

                    # Construct image filename
                    company_name = df.loc[idx]['Symbol'] if 'Symbol' in df.columns else 'Unknown'
                    date_str = idx.strftime("%Y-%m-%d")
                    pattern_names = "_".join(curr_candle['Patterns']).replace(" ", "_")
                    image_filename = f"{company_name}_{date_str}_{pattern_names}.png"
                    image_filepath = os.path.join(image_save_directory, image_filename)

                    # Save image
                    with open(image_filepath, "wb") as img_file:
                        img_file.write(base64.b64decode(img_base64))

                    df.at[idx, 'Pattern_Image'] = image_filepath  # Store image path instead of base64

            except KeyError as e:
                print(f"Skipping pattern at {idx}: {e}")
            except Exception as e:
                print(f"An error occurred at {idx}: {e}")


annotate_patterns_in_charts()
