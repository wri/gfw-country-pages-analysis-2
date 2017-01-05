import pandas as pd
import json
import subprocess
import os

from json_groupby_week import build_week_lookup
from df_to_json import df_to_json
from json_groupby_week import cum_values
from insert_dummyweeks import insert_dummy_cumulative_rows


def write_outputs(records_list, output_s3_path):

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    results_dir = os.path.join(root_dir, 'results')

    basename = os.path.basename(output_s3_path)
    local_json_file = os.path.join(results_dir, basename)

    # add list of dictionaries as value for "data" key
    final_dict = {"data": records_list}

    # write to json
    with open(local_json_file, 'w') as outfile:
        json.dump(final_dict, outfile)

    # add csv file to output
    csv_results = os.path.splitext(local_json_file)[0] + '_processed.csv'

    # write to csv
    df = pd.DataFrame(records_list)
    df.to_csv(csv_results)

    # push to aws
    cmd = ['aws', 's3', 'cp', '--content-type', r'application/json', local_json_file, output_s3_path]
    subprocess.check_call(cmd)


def output_json(pip_result_csv, api_endpoint_object, is_test, climate=False):
    """
    Take the local output from the Hadoop PIP process and write a JSON file
    :param pip_result_csv: the local hadoop PIP CSV
    :param api_endpoint_object: a row from the config sheet:
     https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=923735044
    :param is_test: used to designate if this is a staging run or not-- if so will include more countries in climate
    :param climate: whether or not to run climate processing
    :return:
    """

    # csv to pandas data frame
    field_names = api_endpoint_object.csv_field_names.split(',')
    df = pd.read_csv(pip_result_csv, names=field_names)

    # If we're working with glad alerts, need to do some specific filtering
    # and need to sum by alerts
    if api_endpoint_object.forest_dataset == 'umd_landsat_alerts':

        # filter valid confidence only
        df = df[(df['confidence'] == 2) | (df['confidence'] == 3)]

        # group by day and year, then sum
        groupby_list = ['country_iso', 'state_id', 'day', 'year', 'confidence']

        # df_groupby = df.groupby(groupby_list)['alerts', 'above_ground_carbon_loss', 'area_m2'].sum()
        df_groupby = df.groupby(groupby_list)['alerts', ].sum()

        # df -> list of records
        final_record_list = df_to_json(df_groupby)

    # Custom process/filtering for climate data
    elif climate:

        # filter: confirmed only
        df = df[df['confidence'] == 3]

        # filter: where prf is 1 or where other countries exist
        # don't want to include RUS for climate stuff for now
        country_list = ['BRA', 'PER', 'COG', 'UGA']

        if is_test:
            country_list += ['TLS', 'CMR', 'MYS', 'COD', 'GAB', 'BRN', 'CAF', 'GNQ', 'PNG']

        df = df[(df['prf'] == 1) | (df['country_iso'].isin(country_list))]

        # calculate week number
        df['week'] = df.apply(lambda x: build_week_lookup(x['day'], x['year']), axis=1)

        # if week number is 53, then set year to 2015
        df.ix[df.week == 53, 'year'] = 2015

        # group by week and year, then sum
        groupby_list = ['country_iso', 'state_id', 'week', 'year', 'confidence', 'prf']
        df_groupby = df.groupby(groupby_list)['alerts', 'above_ground_carbon_loss', 'area_m2'].sum()

        # df -> list of records, so we can run the cumulative values
        raw_record_list = df_to_json(df_groupby, climate)

        # cumulate values
        cum_record_list = cum_values(raw_record_list)

        # fill in missing weeks data for glad 2016 only
        final_record_list = insert_dummy_cumulative_rows(cum_record_list)

    # Otherwise the output from hadoop_pip is already summarized for us, just need
    # to put it in [row, row, row, ...] format
    else:
        final_record_list = df.to_dict('records')

    # write outputs to final file
    write_outputs(final_record_list, api_endpoint_object.s3_url)

