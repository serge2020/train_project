""" Script that calculates average train arrival times from train schedule data at
API endpoint  http://rata.digitraffic.fi/api/v1/trains/
@Author Sergejs Nesterovs, Accenture LATC
"""

import sys
import pandas as pd
import time
from datetime import datetime
from statistics import mean

''' Script requires two input arguments: 
1. csv file name containing arrivals data produced by main.py script.
2. station code for which the average arrival time will be calculated from the source data.
'''
file_name = sys.argv[1]
station_code = int(sys.argv[2])

file_path = "./output/" + file_name
# station_code = 1

def change_timestamp(s):
    """ function that performs time travel :)
    From the input timestamp it keeps only the HH:mm:ss data and returns it as Unix epoch time -
    when it was young enough to not have days months or years...
    :param s: timestamp string
    :return: unix epoch time integer
    """
    _to1 = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")
    _to2 = _to1.replace(year=1970, month=1, day=1)
    unix_time = time.mktime(_to2.timetuple())
    return unix_time

if __name__ == "__main__":

    # load arrival data and transform arrival timestamps to Unix epoch time of HH:mm:ss
    df_arrivals = pd.read_csv(file_path)
    _df = df_arrivals.loc[df_arrivals["stationUICCode"] == station_code].copy()
    _df["epoch_hrs"] = _df.actualTime.apply(change_timestamp)

    # calculate average arrival time and convert it back to the timestamp format
    unx_list = _df["epoch_hrs"].values.tolist()
    mean_epoch = mean(unx_list)
    arrival_result = datetime.fromtimestamp(mean_epoch).strftime("%H:%M:%S")

    avg_difference = round(_df["differenceInMinutes"].mean(), 2)

    # print results
    print("Station code: ", station_code)
    print("Average arrival time: ", arrival_result)
    print("Average arrival time difference in minutes: ", avg_difference)


