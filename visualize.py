
from core.data_handler import load_from_hdf
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

class VisualizeBrent:
    def __init__(self):
        self.combined_df = self.combine_data()
        self.normalized_df = self.normalize_dfs()
        self.rolling_weekly = self.rolling_average()
        self.rolling_monthly = self.rolling_average(window=30)
        self.display_labels = ['BZ Futures', 'Brent Crude', 'EUR/USD', 'VIX Index']
        self.display_colors = ['black', 'violet', 'mediumblue', 'lightseagreen']
        self.fig = None
    
    def combine_data(self):
        
        self.df_list = load_from_hdf()
        # join al dfs
        combined_df = self.df_list[0].join(
            self.df_list[1:], how='outer'
        )
        
        # handle missing values
        combined_df.ffill(inplace=True)
        combined_df.bfill(inplace=True)
        combined_df.sort_index(ascending=True, inplace=True)
        combined_df = combined_df[~combined_df.index.duplicated(keep='first')]
        return combined_df
    
    def normalize_dfs(self, factor=100):
        """ normalizes the prices of the different indexes."""
        offset_value = self.combined_df - self.combined_df.min()
        range = self.combined_df.max() - self.combined_df.min()

        return (offset_value / range) * factor
        
        
    def rolling_average(self, window=7, normalized=True):
        if normalized:    
            rolling = self.normalized_df.rolling(window=window).mean()
        else:
            rolling = self.combined_df.rolling(window=window).mean()
        
        return rolling

    @property
    def plot(self):
        plt.show()
        return self.figure

    def long_term_trend_generator(self, ytd=False):
        """generates a figure with 3 subplots for daily, weekly and monthly values
        of the examined indexes, through the entire time period"""

        self.figure, (ax1,ax2,ax3) = plt.subplots(nrows=3, figsize=(14, 12), sharex=True)
        self.figure.suptitle(f'Brent Crude and related indexes value development over 2Y,\n Normalized')
        
        def plot_cols(df, ax):
            for col, color in zip(df.columns, self.display_colors):
                ax.plot(df[col], color=color)
  
        ax1.set_title('Daily Values')
        ax2.set_title('Weekly Rolling Average')
        ax3.set_title('Monthly Rolling Average')
        
        if not ytd:
            
            df1 = self.normalized_df
            df2 = self.rolling_weekly
            df3 = self.rolling_monthly

        else:
            today = pd.to_datetime('today')
            jan_first = pd.to_datetime(f'{today.year}-01-01')

            df1 = self.normalized_df.loc[self.normalized_df.index >= jan_first]
            df2 = self.rolling_weekly.loc[self.rolling_weekly.index >= jan_first]
            df3 = self.rolling_monthly.loc[self.rolling_monthly.index >= jan_first]

        plot_cols(df1, ax1)
        plot_cols(df2, ax2)
        plot_cols(df3, ax3)
        
        ax1.set_ylabel('Daily Normalized Value')
        ax2.set_ylabel('Weekly Value')
        ax3.set_ylabel('Monthly Value')
        ax3.set_xlabel('Date')

        self.figure.legend(*ax2.get_legend_handles_labels(), 
                   labels=self.display_labels, 
                   loc='upper right',
                   shadow=True)
        plt.show()
        return self.figure

    
    def heatmap_generator(self):
        """generates a correlation heatmap for all indexes through the entire time period"""
        ax = sns.heatmap(self.combined_df.corr(), 
                         annot=True, 
                         cmap="crest", 
                         xticklabels=self.display_labels,
                         yticklabels=self.display_labels,
                         linewidths=0.5)
        plt.yticks(rotation=0) 
        plt.title('Indexes Correlation Heatmap')
        self.figure = ax
        return self.figure
    
    def heatmap_volatility(self):
        """generates a heatmap for the volatility of each of the indexes, 
        based on the std of the daily change, in relaton to the monthly-average"""
        # daily changes
        change = self.combined_df.pct_change()

        # Rolling volatility for each column
        rolling_vol = change.rolling(window=30).std()

        # Correlation of volatility between assets
        vol_corr = rolling_vol.corr()
        ax = sns.heatmap(vol_corr, 
                         annot=True, 
                         cmap="crest", 
                         linewidths=0.5,
                         xticklabels=self.display_labels,
                         yticklabels=self.display_labels)
        plt.yticks(rotation=0) 
        plt.title('Volatility Correlation Heatmap')
        plt.show()
        return self.figure

    def recent_volatility_brent(self):
        """get the recent volatility chart
        for each day in the past week, compared to the previous 20 days"""
        #  calculate daily % change
        df = self.combined_df[['brent_value']].sort_index()  
        df['values'] = df.pct_change()

        # claculate 20 days rolling average std for each day
        df['rolling_volatility'] = df['values'].rolling(window=20).std()
        # compare the last 7 days
        recent_vol = df[['rolling_volatility']].dropna().iloc[-7:] * 100

        # plot
        fig, ax = plt.subplots( figsize=(14,8))#, layout='constrained')

        # position for ticker next to the last value
        y_pos = recent_vol.iloc[-1] + 0.02
        x_pos = recent_vol.index[-1]
        ax.text(x=x_pos,y=y_pos,s='Brent Crude', fontsize=12, color='violet', weight='bold')
        ax.plot(recent_vol, color='m')
        ax.fill_between(recent_vol.index, 
                        recent_vol['rolling_volatility'], 
                        recent_vol['rolling_volatility'].min(), color='violet', alpha=0.1)

        plt.title('Brent Crude Rolling Volatility: Daily % change in last 7 Days compared to 20 days window')
        plt.ylabel('Volatility: (std of value)')
        plt.xlabel('Date')
        plt.show()

    def recent_volatility_combined(self):
        """get the recent volatility chart
        for each day in the past week, compared to the previous 20 days"""
        #  calculate daily % change
        df = self.combined_df.sort_index()  
        print(f'original df: \n{df.tail(10)}')
        change = df.pct_change()
        print(f'percent change: \n{change.tail(10)}')
        # claculate 20 days rolling average std for each day
        rolling_vol = change.rolling(window=20).std()
        print(f'rolling volatility 20 days: \n{rolling_vol.tail(10)}')
        # compare the last 7 days
        recent_vol = rolling_vol.dropna().iloc[-7:] * 100
        print(f'only for the last 7 points: {recent_vol}')

        # plot
        fig, ax = plt.subplots( figsize=(14,8))#, layout='constrained')
        
        # give different colors
        x_location = 1
        for col, color, label in zip(recent_vol.columns, self.display_colors, self.display_labels):
            ax.plot(recent_vol[col], color=color)

            # position for ticker next to the last value
            y_pos = recent_vol[col].iloc[- x_location]
            x_pos = recent_vol[col].index[ - x_location] 
            x_location += 1
            ax.text(x=x_pos,y=y_pos,s= label, fontsize=10, color=color, weight='bold')
            
        plt.legend(ax.get_legend_handles_labels(), 
                   labels=self.display_labels, 
                   loc='upper right',
                   shadow=True)        
        plt.title('Indexes Rolling Volatility: Daily % change in last 7 Days compared to 20 days window', weight='bold')
        plt.ylabel('Volatility: (std of value), in %')
        plt.xlabel('Date')   

visual = VisualizeBrent()
plot = visual.recent_volatility_combined()
plt.show()