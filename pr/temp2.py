import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
import numpy as np
import json
import os
from matplotlib.patches import Rectangle
from datetime import datetime

# ======================
# STYLE CONFIGURATION
# ======================
PRO_STYLE = {
    'base_mpl_style': 'seaborn-darkgrid',
    'marketcolors': mpf.make_marketcolors(
        up='#2ecc71',  # Green
        down='#e74c3c',  # Red
        wick={'up': '#2ecc71', 'down': '#e74c3c'},
        edge={'up': '#2ecc71', 'down': '#e74c3c'},
        volume='in'
    ),
    'mavcolors': ['#3498db', '#f1c40f'],  # Moving average colors
    'facecolor': '#ecf0f1',  # Chart background
    'gridcolor': '#bdc3c7',  # Grid lines color
    'gridstyle': '--',  # Grid line style
    'rc': {
        'axes.labelcolor': '#2c3e50',
        'text.color': '#2c3e50',
        'font.size': 10,
        'axes.titlesize': 12,
        'axes.labelsize': 10
    },
    'y_on_right': True,  # Fix for y_on_right error
    'figscale': 1.2,  # Figure scaling
    'volume_panel': 1  # Volume panel position
}

PATTERN_COLORS = {
    'Engulfing': '#2980b9',
    'Hammer': '#27ae60',
    'ShootingStar': '#c0392b',
    'Doji': '#f1c40f',
    'InvertedHammer': '#8e44ad',
    'PiercingLine': '#2ecc71',
    'DarkCloudCover': '#e74c3c'
}


# ======================
# CHART GENERATION
# ======================
def create_pro_chart(data, pattern_date, patterns, symbol, save_dir):
    try:
        # Data preparation
        idx = data.index.get_loc(pattern_date)
        start_idx = max(0, idx - 20)
        end_idx = min(len(data), idx + 5)
        subset = data.iloc[start_idx:end_idx].copy()  # Avoid SettingWithCopyWarning

        # Create figure
        fig = plt.figure(figsize=(12, 8), dpi=150, facecolor=PRO_STYLE['facecolor'])
        gs = fig.add_gridspec(6, 1)
        ax_price = fig.add_subplot(gs[:4, 0])
        ax_vol = fig.add_subplot(gs[4, 0], sharex=ax_price)
        ax_info = fig.add_subplot(gs[5, 0])

        # Plot candlestick chart
        mpf.plot(subset, type='candle', ax=ax_price, volume=ax_vol,
                 style=PRO_STYLE, show_nontrading=False)

        # Pattern annotations
        pattern_x = len(subset) - 1
        candle = subset.iloc[-1]
        price_range = candle['High'] - candle['Low']

        # Annotate each pattern
        for pattern in patterns:
            color = PATTERN_COLORS.get(pattern, '#7f8c8d')

            # Background highlight
            ax_price.add_patch(Rectangle(
                (pattern_x - 0.4, candle['Low']), 0.8, price_range,
                color=color, alpha=0.1
            ))

            # Pattern marker
            ax_price.plot(pattern_x, candle['Close'],
                          marker='*' if pattern == 'ShootingStar' else 'd',
                          markersize=12,
                          color=color,
                          markeredgecolor='white',
                          markeredgewidth=1.5)

            # Text annotation
            ax_price.annotate(pattern.upper(),
                              xy=(pattern_x, candle['High'] * 1.02),
                              xytext=(pattern_x + 1.5, candle['High'] * 1.08),
                              arrowprops=dict(
                                  arrowstyle="fancy",
                                  color=color,
                                  connectionstyle="angle3,angleA=0,angleB=90"
                              ),
                              fontsize=10,
                              weight='bold',
                              color=color)

        # Technical indicators
        for period in [5, 20]:
            subset[f'SMA{period}'] = subset['Close'].rolling(period).mean()
            ax_price.plot(subset[f'SMA{period}'],
                          color=PRO_STYLE['mavcolors'][0 if period == 5 else 1],
                          lw=1.5,
                          label=f'SMA {period}')

        # Volume bars
        ax_vol.bar(subset.index, subset['Volume'],
                   color=np.where(subset['Close'] >= subset['Open'],
                                  PRO_STYLE['marketcolors']['up'],
                                  PRO_STYLE['marketcolors']['down']),
                   alpha=0.7)

        # Info panel
        ax_info.axis('off')
        info_text = (
            f"SYMBOL: {symbol}\nDATE: {pattern_date.strftime('%Y-%m-%d')}\n"
            f"OPEN: {candle['Open']:.2f}\nHIGH: {candle['High']:.2f}\n"
            f"LOW: {candle['Low']:.2f}\nCLOSE: {candle['Close']:.2f}\n"
            f"PATTERNS: {', '.join(patterns)}"
        )
        ax_info.text(0.05, 0.5, info_text, fontsize=9,
                     va='center', linespacing=1.8,
                     bbox=dict(facecolor='white', alpha=0.8, edgecolor='#bdc3c7'))

        # Save image
        filename = f"{symbol}_{pattern_date.strftime('%Y%m%d')}_{'_'.join(patterns)}.png"
        filepath = os.path.join(save_dir, filename)
        plt.savefig(filepath, bbox_inches='tight', pad_inches=0.5)
        plt.close()

        return filepath

    except Exception as e:
        print(f"Chart generation error: {str(e)}")
        return None


# ======================
# DATA PROCESSING
# ======================
def list_files_in_directory(directory):
    return [os.path.join(root, file)
            for root, _, files in os.walk(directory)
            for file in files if file.endswith('.csv')]


def validate_patterns(pattern_entry):
    try:
        if isinstance(pattern_entry, list):
            return pattern_entry
        return json.loads(pattern_entry)
    except:
        return []


def annotate_patterns_in_charts(input_directory=r"D:\image",
                                output_csv=r"D:\database_final\output_with_images.csv",
                                image_save_directory=r"D:\pattern_images"):
    os.makedirs(image_save_directory, exist_ok=True)
    all_files = list_files_in_directory(input_directory)

    for file_path in all_files:
        try:
            df = pd.read_csv(file_path, parse_dates=['Date'], index_col=['Date'])
            df['Patterns'] = df['Patterns'].apply(validate_patterns)

            if 'Pattern_Image' not in df.columns:
                df['Pattern_Image'] = np.nan

            pattern_dates = df[df['Patterns'].apply(len) > 0].index

            for date in pattern_dates:
                try:
                    raw_patterns = df.loc[date, 'Patterns']
                    patterns = [p for p in raw_patterns if isinstance(p, str) and p in PATTERN_COLORS]

                    if not patterns:
                        continue

                    symbol = df.loc[date, 'Symbol'] if 'Symbol' in df.columns else 'UNKNOWN'
                    img_path = create_pro_chart(df, date, patterns, symbol, image_save_directory)

                    if img_path:
                        df.at[date, 'Pattern_Image'] = img_path

                except Exception as e:
                    print(f"Error processing {date}: {str(e)}")

        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")


if __name__ == "__main__":
    annotate_patterns_in_charts()
