import pandas as pd
import csv

raw_datalines = pd.read_csv("raw_data.txt", index_col=False, header=None).iloc[:, 0].squeeze()
raw_datalines = raw_datalines.str.strip().tolist()
raw_datalines = raw_datalines[9:]

timestamps = [item[0:8] for item in raw_datalines]

taggers_and_tagged = [item[8:].split(' ') for item in raw_datalines]
taggers = [item[0][3:] for item in taggers_and_tagged]
tagged = [item[2][3:] for item in taggers_and_tagged]

complete_time_series_df = pd.DataFrame({
    'timestamp': timestamps,
    'tagger': taggers,
    'tagged': tagged}).sort_values(by='timestamp')

# Remove earlier row if two rows have same 'tagger' and 'tagged' (in any order) and timestamps are 0 or 1 second apart
# --------------------LLM Generated Code--------------------
complete_time_series_df['timestamp_dt'] = pd.to_datetime(complete_time_series_df['timestamp'], format='%H:%M:%S')
complete_time_series_df = complete_time_series_df.sort_values(by=['timestamp_dt', 'tagger', 'tagged']).reset_index(drop=True)

def is_duplicate_or_swapped(row, prev_row):
    if prev_row is None:
        return False
    time_diff = (row['timestamp_dt'] - prev_row['timestamp_dt']).total_seconds()
    same_pair = (
        (row['tagger'] == prev_row['tagger'] and row['tagged'] == prev_row['tagged']) or
        (row['tagger'] == prev_row['tagged'] and row['tagged'] == prev_row['tagger'])
    )
    return same_pair and time_diff in (0, 1)

mask = [False]
for i in range(1, len(complete_time_series_df)):
    row = complete_time_series_df.iloc[i]
    prev_row = complete_time_series_df.iloc[i-1]
    mask.append(is_duplicate_or_swapped(row, prev_row))

complete_time_series_df = complete_time_series_df[~pd.Series(mask).shift(-1, fill_value=False)].reset_index(drop=True)
complete_time_series_df = complete_time_series_df.drop(columns=['timestamp_dt'])
# --------------------LLM Generated Code--------------------

complete_time_series_df = complete_time_series_df.sort_values(by='timestamp')
complete_time_series_df.to_csv("processed_timeseries.csv", index=False)