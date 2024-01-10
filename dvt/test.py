import pandas as pd

latest_path = "C:\\Users\\60099\\code\\dvt\\validations\\sapwest_EVER\\latest_summary_sapwest_EVER.pkl"
df = pd.read_pickle(latest_path)

#if thread_cnt<len(df):
#    df = df.iloc[:-2]

print(df)
