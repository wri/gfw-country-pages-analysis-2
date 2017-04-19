import os
import json
from datetime import datetime


def load_json_from_token(file_name):

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    token_file = os.path.join(root_dir, 'tokens', file_name)

    with open(token_file) as data_file:
        data = json.load(data_file)

    return data


def df_year_day_to_month(df):
    as_date = datetime.strptime(str(df.year) + str(df.day), '%Y%j')

    return as_date.month

