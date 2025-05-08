from core.data_handler import load_from_hdf
import pandas as pd


df_list = load_from_hdf()
for df in df_list:
    print(df.head(15))