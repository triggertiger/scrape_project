from core.data_handler import load_from_hdf
import pandas as pd


df_list = load_from_hdf()
print(df_list[0].head())