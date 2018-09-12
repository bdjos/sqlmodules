# -*- coding: utf-8 -*-
"""
Created on Wed Aug 15 12:27:53 2018

@author: BJoseph
"""

import json
import time
import urllib.request
import datetime
import pandas as pd
import os

API = 'S3e3rqxIt7oAVk1menKx0hAjqz2Wd6tD'
LOCATION_ID = '55488'
FRANGE = 12
direct = 'forecast'

def get_weather(api = 'S3e3rqxIt7oAVk1menKx0hAjqz2Wd6tD', location_id = '55488', frange = 12):
    
    url = f'http://dataservice.accuweather.com/forecasts/v1/hourly/12hour/{LOCATION_ID}?apikey=%09{API}&details=True&metric=True'
    print(url)
    with urllib.request.urlopen(url) as url:
        data = json.loads(url.read().decode())
    print(data)
    
    time, temp, rel_hum, dewpoint = [], [], [], []
    for i in range(12):
        time.append(data[i]['EpochDateTime'])
        temp.append(data[i]['Temperature']['Value'])
        rel_hum.append(data[i]['RelativeHumidity'])
        dewpoint.append(data[i]['DewPoint']['Value'])
        
    df_vals = {'Date/Time': time, 'Temp': temp, 'Dew Point Temp': dewpoint, 'Rel Hum (%)': rel_hum}
    df = pd.DataFrame(df_vals)
    df['Date/Time'] = df['Date/Time'].apply(lambda x: datetime.datetime.fromtimestamp(x))
    df['Year'] = df['Date/Time'].apply(lambda x: x.year)
    df['Month'] = df['Date/Time'].apply(lambda x: x.month)
    df['Day'] = df['Date/Time'].apply(lambda x: x.day)  
    df['Hour'] = df['Date/Time'].apply(lambda x: x.hour)
    
    return df

if __name__ == '__main__':
    timestamp = time.time()
    df = get_weather(API, LOCATION_ID, FRANGE)
    today = datetime.datetime.today()
    datestr = f'{today.year}-{today.month}-{today.day}'
    
    forecast_file = os.path.join(direct, f'forecast{datestr}.csv')
    df.to_csv(forecast_file, index=False)
