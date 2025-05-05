import os
from dotenv import load_dotenv
import sys
import pandas as pd
from functools import wraps
import hashlib
import pickle
from datetime import datetime, time, timedelta

folder_path = os.path.dirname(__file__)
data_folder_path = os.path.join(folder_path, 'cache')
if not os.path.exists(data_folder_path):
  os.makedirs(data_folder_path)
  print("Creating data folder...")

env_path = os.getenv('BROKER_ENV_FILE_PATH')
if env_path is not None and os.path.exists(env_path):
  print(load_dotenv(env_path))
else:
  print("Error :: Env file missing for Broker ", env_path)
  print("Exiting safely")
  sys.exit()
  
# This class is to accumulate configs
class Config:
    BROKERCONFIG = {
        "api_key": os.getenv("INTRA_API_KEY"),
        "secret_key": os.getenv("INTRA_SECRET_KEY"),
        "pin": os.getenv("INTRA_PIN"),
        "clientId": os.getenv("INTRA_CLIENT_ID"),
        "angletoken" : os.getenv("ANGLETOKEN"),
    }

    TELEGRAM = {
        "token": os.getenv("TELEGRAM_TOKEN"),
        "chatid": os.getenv("TELEGRAM_CHATID"),
    }

from broker import AngelOne
mybroker = AngelOne(Config.BROKERCONFIG)
status = mybroker.connect()
if status is False:
  print("unable to connect broker")
  sys.exit(1)

cache_dir = data_folder_path

class FileDict:
    def __init__(self, path):
        self.path = path
        os.makedirs(path, exist_ok=True)  # Create the directory if it doesn't exist
    
    def _get_file_path(self, func_key, arg_hash):
        """Generate the file path for a given function key and argument hash."""
        filename = f"{func_key}_{arg_hash}.pkl"
        return os.path.join(self.path, filename)
    
    def get(self, func_key, arg_hash):
        """Retrieve the value from the cache if it exists, otherwise raise KeyError."""
        file_path = self._get_file_path(func_key, arg_hash)
        if not os.path.exists(file_path):
            raise KeyError(f"No cache found for key: {func_key} with hash: {arg_hash}")
        with open(file_path, 'rb') as file:
            return pickle.load(file)
    
    def set(self, func_key, arg_hash, value):
        """Store the value in the cache."""
        file_path = self._get_file_path(func_key, arg_hash)
        with open(file_path, 'wb') as file:
            pickle.dump(value, file)
    
    def contains(self, func_key, arg_hash):
        """Check if a cached value exists for the given function key and argument hash."""
        file_path = self._get_file_path(func_key, arg_hash)
        return os.path.exists(file_path)
    
def month_end_day(scanday):
  if scanday.month<=11:
    return datetime(scanday.year, scanday.month+1, 1) - timedelta(1)
  if scanday.month>11:
    return datetime(scanday.year+1, 1, 1) - timedelta(1)
  
def is_same_month(scanday):
  if scanday.year == datetime.today().year and scanday.month == datetime.today().month:
      return True
  return False

def is_same_day(scanday):
    if scanday.date() == datetime.today().date():
        return True
    return False

def filter_by_day(df, day):
  df = df[df['timestamp'].apply(lambda x:x[:10]) == day.strftime('%Y-%m-%d')]
  df.reset_index(drop=True, inplace = True)
  return df


def load_or_save_dataframe(subdir=None, save_type='pkl', non_empty=False):
    file_cache = FileDict(os.path.join(cache_dir, subdir))
    def decorator(func):
        @wraps(func)
        def wrapper(exchange, symbol, token,  scanday, *args, **kwargs):
              if is_same_month(scanday):
                print('is same day', scanday)
                result = func(exchange, symbol, token, scanday, *args, **kwargs)
                return result
              
              if not is_same_month(scanday):
                scanday = month_end_day(scanday)
              else:
                scanday = datetime.today()
             
              func_key = f"{func.__module__}.{func.__name__}"
              arg_key = f"{symbol}:{scanday.date()}:{args}:{kwargs}"
              arg_hash = f"{symbol}_{scanday.date()}_" + hashlib.md5(arg_key.encode()).hexdigest()

              if file_cache.contains(func_key, arg_hash):
                  existing_df = file_cache.get(func_key, arg_hash)
                  return existing_df

              df = func(exchange, symbol, token, scanday, *args, **kwargs)
              if df is not None:
                  file_cache.set(func_key, arg_hash, df)
              return df
        return wrapper
    return decorator

# Sync function to pull historical stock data
@load_or_save_dataframe('data')
def sync(exchange, symbol, token, scanday):
    print('Syncing for Day - ', scanday)
    endday = None
    startday = None
    if not is_same_month(scanday):
      startday = datetime(scanday.year, scanday.month, 1)
    else:
      startday = datetime(scanday.year, scanday.month, 1)
    endday = month_end_day(scanday)
    response = mybroker.get_candle_stick_data(exchange,symbol, token, 'ONE_MINUTE', startday, endday)
    if response['data'] is not None:
        data = pd.DataFrame(response["data"], columns=["timestamp", "open", "high", "low", "close", "volume"])
        return data[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
    else:
        print("No data returned from API.")
        return None



def fetch_data(exhange, symbol, token, startdate, enddate):
  if startdate > enddate:
    print(f"Unable to sync incorrect datetime :{symbol} {startdate} -> {enddate}")
    return None
  
  groupmonths = []
  enddate = month_end_day(enddate)
  while(startdate < enddate):
    startdate = month_end_day(startdate)
    df = sync(exhange, symbol, token, startdate)
    if df is not None:
      groupmonths.append(df)      
    startdate = startdate + timedelta(1)
  if len(groupmonths)>0:
    df = pd.concat(groupmonths)
    df.reset_index(inplace=True, drop=True)
    return df
  return None