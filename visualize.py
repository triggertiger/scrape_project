
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
        self.display_colors = ['b', 'm', 'g', 'r']
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

        return combined_df
    
    def normalize_dfs(self, factor=100):
        """ normalizes the prices of the different indexes."""
        offset_value = self.combined_df - self.combined_df.min()
        range = self.combined_df.max() - self.combined_df.min()

        return (offset_value / range) * factor
        
        
    def rolling_average(self, window=7, normalized=True):
        print(f'normalized from within the rolling average: {self.normalized_df}')
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
        self.figure, (ax1,ax2,ax3) = plt.subplots(nrows=3, figsize=(14, 12), sharex=True)
        self.figure.suptitle(f'Brent Crude and related indexes value development over 2Y,\n Normalized')
        
        def plot_cols(df, ax):
            for col, color in zip(df.columns, self.display_colors):
                ax.plot(df[col], color=color)
  
        ax1.set_title('Daily Values')
        ax2.set_title('Weekly Rolling Average')
        ax3.set_title('Monthly Rolling Average')
        
        if ytd:
            
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
        #self.figure = fig
        return self.figure

    
    def heatmap_generator(self):
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
        # std correlation:
        returns = self.combined_df.pct_change()

        # Rolling volatility for each column
        rolling_vol = returns.rolling(window=30).std()

        # Correlation of volatility between assets
        vol_corr = rolling_vol.corr()
        ax = sns.heatmap(vol_corr, 
                         annot=True, 
                         cmap="crest", 
                         linewidths=0.5,
                         xticklabels=self.display_labels,
                         yticklabels=self.display_labels)
        plt.yticks(rotation=0) 
        plt.title('Indexes Correlation Heatmap')
        plt.show()
        return self.figure

    # def short_term_monthly_drawdown(self):
    #     series = combined
    #     roll_max = series.cummax()
    #     drawdown = (series - roll_max) / roll_max

    def recent_volatility(self):
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
        ax.text(x=x_pos,y=y_pos,s='Brent Crude', fontsize=12, color='m')
        ax.plot(recent_vol, color='m')
        ax.fill_between(recent_vol.index, 
                        recent_vol['rolling_volatility'], 
                        recent_vol['rolling_volatility'].min(), color='m', alpha=0.1)

        plt.title('Brent Crude Rolling Volatility: Daily % change in last 7 Days compared to 20 days window')
        plt.ylabel('Volatility: (std of value)')
        plt.xlabel('Date')

        

        plt.show()

     

visual = VisualizeBrent()
plot = visual.recent_volatility()
#visual.figure.show()
plt.show()