import pytz
from dateutil.parser import isoparse
import requests
from http.client import responses
import pandas as pd
from tabulate import tabulate
from flatten_json import flatten
import os
import json


def add_sydney_timezone(*, input_datetime):
    # Add timezone to input datetime in format required by Mambu
    if input_datetime == "NaT":
        return None
    else:
        naive_datetime = isoparse(input_datetime)
        syd_timezone = pytz.timezone("Australia/Sydney")
        syd_aware_datetime = syd_timezone.localize(naive_datetime)
        iso_formatted_string = syd_aware_datetime.isoformat("T", "seconds")
        return iso_formatted_string


def flatten_json_to_df(data):
    # Flatten json response to Pandas DataFrame
    dic_flattened = flatten(data)
    df = pd.DataFrame([dic_flattened])
    return df


def print_tabulate(data):
    # Simple function to enhance default print function to be more readable
    print(tabulate(data, headers="keys", tablefmt="psql"))


def get_sql(path, file_name):
    with open(os.path.join(path, f"sql/{file_name}.sql")) as file:
        sql = file.read()
    return sql


def convert_df_to_json(df):
    json_data = {}

    for z in df.index:
        json_data[z] = df.iloc[z].dropna().to_dict()

    json_str = json.dumps(json_data, indent=4, sort_keys=True)
    return json_str
