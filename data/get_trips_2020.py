import os
import json
import calendar
import logging

#!pip install sodapy
from sodapy import Socrata

def get_trips_data(month):
    """
    Requests trips data from the city of Chicago for a single month
    """

    url = "data.cityofchicago.org"
    client = Socrata(url, None, timeout=60)

    # Dataset: Transport Network Provider City of Chicago 
    # https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips-2018-2022-/m6dm-c72p
    identifier = "m6dm-c72p"
    year = 2020 # Specify year here!

    end_of_month = calendar.monthrange(year, month)

    for i in range(1, end_of_month[1] + 1):
        logging.info(f"Retrieving trips data, {year}-{month}-{i}")
        trips = client.get(identifier, where=f"trip_start_timestamp >= '{year}-{month}-{i}T00:00:00.000' AND trip_start_timestamp <= '{year}-{month}-{i}T23:59:59.999'", limit=500000, order="trip_start_timestamp")
        
        logging.info(f"Writing file, {year}-{month}-{i}")
        filename = f"data/trips_data/{year}/{month}/trips_{year}_{month}_{i}.json"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as f:
            json.dump(trips, f)

        logging.info(f"Trips recorded, {year}-{month}-{i}: {len(trips)}")
        print(f"Trips recorded, {year}-{month}-{i}: {len(trips)}")


def main():
    for i in range(1, 13):
        get_trips_data(i)

if __name__ == "__main__":
    main()