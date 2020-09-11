""" Script that loads data from the Finnish Transport Agency train schedule
API endpoint  http://rata.digitraffic.fi/api/v1/trains/ , normalizes data following star schema principles and saves
entity data to csv files in the project "output" folder.
@Author Sergejs Nesterovs, Accenture LATC
"""

import sys
import requests
from datetime import datetime, timedelta
import pandas as pd

''' Script requires three input arguments: train number, start and end dates of the period for which the data
 will be loaded. dates mus be provided in YYYY-MM-dd format.
'''
train_no = sys.argv[1]
start_date = sys.argv[2]
end_date = sys.argv[3]

# end_date = "2020-07-31"
# start_date = "2020-07-01"
'''
 Setting variables used in script.
'''
date_format = "%Y-%m-%d"
date_format_csv = "%Y%m%d"
root_api = "https://rata.digitraffic.fi/api/v1/trains/"
# train_no = "4"
output_path = "./output/"
'''
Setting table schema information (see README file project repository)
'''
tt_key = "timeTableRows"  # api json key for timetable data
tready_key = "trainReady"  # api json key for trainReady data
# assigning table schema information
causes_cols = ["actualTime", "type", "stationUICCode", "categoryCode", "detailedCategoryCode", "thirdCategoryCode",
               "categoryCodeId", "detailedCategoryCodeId", "thirdCategoryCodeId"]
tt_cols = ["scheduledTime", "actualTime", "stationUICCode", "type", "trainStopping", "commercialStop",
           "commercialTrack", "cancelled", "differenceInMinutes", "liveEstimateTime", "estimateSource"]
st_cols = ["stationUICCode", "stationShortCode", "countryCode"]
# variables used to process fct_arrival and fct_departure tables
type_col = "type"
type_a = "ARRIVAL"
type_d = "DEPARTURE"


def set_url_list(base_url, start, end, train, d_fmt, ):
    """ function that generates url list to be used for data extraction rom api enpoint. It requires input parameters
    :param base_url: api base url address string
    :param start: period start date in YYYY-MM-dd format
    :param end: period end date in YYYY-MM-dd format
    :param train: train number in string format
    :param d_fmt: datetime format string
    :return: list of url addresses
    """
    url_list = []
    end_ts = datetime.strptime(end, d_fmt)
    start_ts = datetime.strptime(start, d_fmt)
    _date = start_ts
    while _date <= end_ts:
        url_string = base_url + _date.strftime(date_format) + "/" + train
        url_list.append(url_string)
        _date += timedelta(days=1)
    return url_list


def get_df_sum(api_response, drop_cols, json_obj):
    """ function that creates dataframe with "dim_sumary" table data
    :param api_response: content of api response
    :param drop_cols: fields ("timeTableRows") that have to be removed from the resulting dataframe
    :param json_obj: json object ("trainReady) that has to be added to the resulting dataframe
    :return: dataframe with "dim_sumary" table data
    """
    sum_tbl = api_response.json()
    _df_sum = pd.json_normalize(sum_tbl)
    ''' data under "trainReady" key in response json can be added to "dim_summary" table since there's only one
     such object in the daily data  '''
    tready_json = response.json()[0].get(drop_cols)[0].get(json_obj)
    df_tready = pd.json_normalize(tready_json)
    _df_sum = _df_sum.drop(drop_cols, axis=1)
    df_sum = pd.concat([_df_sum, df_tready], axis=1)
    return df_sum


def get_df_tt_full(api_response, tt_key):
    """ function that creates dataframe containing all data of "timeTableRows" json object
    :param api_response: content of api response
    :param tt_key: json key ("timeTableRows")
    :return: dataframe
    """
    tt_rows = api_response.json()[0].get(tt_key)
    df_tt = pd.json_normalize(tt_rows)
    return df_tt


def get_df_tt(df, select_cols, f_col, f_val):
    """ function that processed data for "fct_arrival" and "fct_departure" tables
    :param df: dataframe containing all timeTableRows data
    :param select_cols: list of columns to be saved in the resulting dataframes
    :param f_col: column for datafiltering ("type")
    :param f_val: filtering value parameter
    :return: dataframe
    """
    _df_tt = df.filter(items=select_cols, axis=1)
    _df_tt = _df_tt.loc[_df_tt[f_col] == f_val]
    df_tt = _df_tt.drop(f_col, axis=1)
    return df_tt


def get_df_station(df, select_cols):
    """ function that processed data for "dim_station" table
    :param df: dataframe containing all "timeTableRows" object data
    :param select_cols: list of columns to be saved in the resulting dataframes
    :return: dataframe
    """
    df_st = df.filter(items=select_cols, axis=1)
    return df_st


def get_df_cases(api_response, tt_key):
    """ function that processed data for "dim_case" table
    :param api_response: content of api response
    :param tt_key: json key ("timeTableRows")
    :return: dataframe
    """
    causes_list = []
    df_causes = pd.DataFrame()
    tt_rows = api_response.json()[0].get(tt_key)
    #  Iterate over "timeTableRows" json child objects and retrieve data of "causes" key  where it is present
    for x in range(len(tt_rows)):
        _causes = tt_rows[x].get("causes")
        if len(_causes) > 0:
            row_dict = tt_rows[x]  # .items() #[1, 3, 9]
            st_code = row_dict.get("stationUICCode")
            type = row_dict.get("type")
            ts = row_dict.get("actualTime")
            row_dict_filtered = {
                "actualTime": ts,
                "type": type,
                "stationUICCode": st_code
            }
            _causes = tt_rows[x].get("causes")
            row_dict_filtered.update((_causes[0]))
            causes_list.append(row_dict_filtered)
    # load list of retrieved "cases" objects into dataframe
    if len(causes_list) > 0:
        df_causes = pd.DataFrame(causes_list, columns=causes_cols)

    return df_causes


def get_date(ts):
    """ function that is used as dataframe column mapping/lambda function for.
     It parses input timestamp string to the required format
    :param ts: timestamp string
    :return: parsed timesamp string
    """
    _ts = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
    parsed_date = datetime.strftime(_ts, "%Y-%m-%d")
    return parsed_date


if __name__ == "__main__":

    # Create url list
    url_list = set_url_list(root_api, start_date, end_date, train_no, date_format)

    df_summary = pd.DataFrame()
    df_arrival = pd.DataFrame()
    df_departure = pd.DataFrame()
    df_stations = pd.DataFrame()
    df_cases = pd.DataFrame()
    print("Requesting data from API endpoint and processing dataframes...")
    # Iterate over url list
    for url in url_list:
        #get api response
        try:
            response = requests.get(url)
        except EnvironmentError:
            print("API error calling url ", url)
            response = b"[]"
        if response.content == b"[]":
            continue
        # load daily data in temporary dataframes corresponding to defined schemas
        df_timetable = get_df_tt_full(response, tt_key)
        df_1 = get_df_sum(response, tt_key, tready_key)
        df_2 = get_df_tt(df_timetable, tt_cols, type_col, type_a)
        df_3 = get_df_tt(df_timetable, tt_cols, type_col, type_d)
        df_4 = get_df_station(df_timetable, st_cols)
        df_5 = get_df_cases(response, tt_key)

        # append daily data to final dataframes
        df_summary = df_summary.append(df_1)
        df_arrival = df_arrival.append(df_2)
        df_departure = df_departure.append(df_3)
        df_stations = df_stations.append(df_4)
        if not df_5.empty:
            df_cases = df_cases.append(df_5)
    df_stations = df_stations.drop_duplicates()
    print("Saving csv files...")

    # Save final dataframes to csv files
    # generate period string for the file name
    period = datetime.strptime(start_date, date_format).strftime(date_format_csv) + "-" \
             + datetime.strptime(end_date, date_format).strftime(date_format_csv)

    df_summary.to_csv(output_path + "summary_" + period + ".csv", index=False)
    print("summary_" + period + ".csv saved to output folder")
    df_arrival.to_csv(output_path + "arrival_" + period + ".csv", index=False)
    print("arrival_" + period + ".csv saved to output folder")
    df_departure.to_csv(output_path + "departure_" + period + ".csv", index=False)
    print("departure_" + period + ".csv saved to output folder")
    df_stations.to_csv(output_path + "station_" + period + ".csv", index=False)
    print("stations_" + period + ".csv saved to output folder")
    df_cases.to_csv(output_path + "case_" + period + ".csv", index=False)
    print("cases_" + period + ".csv saved to output folder")

    # ONE BONUS STEP
    # From the extracted data in normalized tables let's create one denormalized table containing all the data
    # It might become handy for loading this data into distributed columnar stores such as Google BigQuery.

    # load csv files
    df_summary = pd.read_csv(f"./output/summary_{period}.csv")
    df_arrival = pd.read_csv(f"./output/arrival_{period}.csv")
    df_departure = pd.read_csv(f"./output/departure_{period}.csv")
    df_stations = pd.read_csv(f"./output/station_{period}.csv")
    df_cases = pd.read_csv(f"./output/case_{period}.csv")

    # prepare dataframes for joining
    _df_sum = df_summary.copy()
    _df_sum["cancelled_sum"] = _df_sum["cancelled"]
    _df_sum = _df_sum.drop("cancelled", axis=1)
    _df_arr = df_arrival.copy()
    _df_arr["type"] = "ARRIVAL"
    _df_dpt = df_departure.copy()
    _df_dpt["type"] = "DEPARTURE"

    # create union of "fct_arrival" and "fct_departure"
    _df_tt = _df_arr.append(_df_dpt)
    # join dimensonaltables to the fact table
    _df_tt = _df_tt.sort_values(by=["scheduledTime", "type"], axis=0)
    _df_tt["date"] = _df_tt.scheduledTime.apply(get_date)
    _df_tt = _df_tt.merge(df_stations, how="left", on="stationUICCode")
    _df_tt = _df_tt.merge(df_cases, how="left", on=["actualTime", "type"])
    _df_tt["stationUICCode"] = _df_tt["stationUICCode_x"]
    _df_tt = _df_tt.drop(["stationUICCode_x", "stationUICCode_y"], axis=1)
    _df_tt = _df_tt.merge(_df_sum, how="left", left_on="date", right_on="departureDate")
    df_final = _df_tt.drop(["departureDate"], axis=1)

    # save denormalized table to csv.
    df_final.to_csv(output_path + "denormalized_" + period + ".csv", index=False)
    print("denormalized_" + period + ".csv saved to output folder")
