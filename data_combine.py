import pandas as pd
import os
import glob

path = os.path.dirname(__file__)

def load_csv():
    # get all csv files beginning with 2019 or 2020
    frames = []
    # for name in glob.glob(f"{path}/data/2019*.csv"):
    #     df = pd.read_csv(name)
    #     frames.append(df)
    for name in glob.glob(f"{path}/data/2020*.csv"):
        print(name)
        df = pd.read_csv(name)
        frames.append(df) 

    return pd.concat(frames)

df = load_csv()
df.to_csv(f"{path}/data/combined_2020.csv", index=False)
