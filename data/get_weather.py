import os
from datetime import datetime
import pandas as pd

#!pip install meteostat
from meteostat import Hourly, Point

def get_weather():
    """
    Fetches daily weather information from https://dev.meteostat.net/
    """
    
    year = 2020
    
    start = datetime(year, 1, 1)
    end = datetime(year, 12, 31, 23, 59)

    chicago = Point(41.85003, -87.65005, 181)

    data = Hourly(chicago, start, end)
    data = data.fetch()

    df = pd.DataFrame(data)

    filename = f"data/weather_{year}.csv" 
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    df.to_csv(filename, sep=',')

def main():
    get_weather()

if __name__ == "__main__":
    main()