import pandas as pd
import os
import datetime



path = os.path.dirname(__file__)

def load_df():
    df = pd.read_csv(f"{path}/data/combined_2020.csv", parse_dates=['starttime', 'stoptime'])
    return df

def clean_df(df):
    # add age
    year = datetime.date.today().year
    df["age_of_rider"] = year - df["birth year"]

    # add Month
    df["Month"] = df["starttime"].dt.strftime("%B")

    # identify over 85 age
    df = df[df["age_of_rider"] <= 85]

    # station names do not contain 'temporarily removed'
    df = df[~df['start station name'].str.contains("temporarily removed")]
    df = df[~df['start station name'].str.contains("(temp)")]
    df = df[~df['end station name'].str.contains("temporarily removed")]

    # identify records where start and stope station are same and trip duration < 2 minutes
    df = df.loc[(~((df["start station name"] == df["end station name"]) & (df["tripduration"] < 90 )))]

    return df

def build_counts(new_df):
    # nothing yet
    df_start = new_df.groupby(['start station latitude', 'start station longitude', 'start station name', 'Month']).agg({'start station id': ['count']})
    df_start.columns = ['start_count']
    df_start = df_start.reset_index()
    df_start.columns = ['latitude', 'longitude', 'station name', 'Month', 'start_count']
    df_end =  new_df.groupby(['end station latitude', 'end station longitude', 'Month']).agg({'end station id': ['count']})
    df_end.columns = ['end_count']
    df_end = df_end.reset_index()
    df_end.columns = ['latitude', 'longitude', 'Month', 'end_count']

    joined_df = pd.merge(df_start, df_end, how='left', left_on=['latitude', 'longitude', 'Month'], right_on=['latitude', 'longitude', 'Month'])
    print(joined_df)
    return joined_df

df = load_df()

new_df = clean_df(df)
loc_counts_month_df = build_counts(new_df)

loc_counts_month_df.to_csv(f"{path}/data/station_counts.csv", index=False)

new_df.to_csv(f"{path}/data/combined_2020_clean.csv", index=False)