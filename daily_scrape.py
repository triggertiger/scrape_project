
import os 
from core.data_handler import scrape_orchestrator, scrape_factory

alpha_api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
df_list = scrape_orchestrator(alpha_api_key, hist=False)
print(df_list)