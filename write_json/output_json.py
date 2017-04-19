import pandas as pd
import json
import subprocess
import os

import json_groupby_week
import df_to_json
import insert_dummyweeks
from utilities import util


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


def output_json(pip_result_csv, api_endpoint_object, environment, climate=False):
    """
    Take the local output from the Hadoop PIP process and write a JSON file
    :param pip_result_csv: the local hadoop PIP CSV
    :param api_endpoint_object: a row from the config sheet:
     https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=923735044
    :param environment: used to designate if this is a staging run or not-- if so will include more countries in climate
    :param climate: whether or not to run climate processing
    :return:
    """
    print 'starting starting read CSV'

    # csv to pandas data frame
    field_names = api_endpoint_object.csv_field_names.split(',')

    # grab any custom datatype specifications from the API spreadsheet
    dtype_dict = json.loads(api_endpoint_object.dtypes)

    # convert them to format {'col_name': <python type>}
    dtype_val = {col_name: eval(col_type) for col_name, col_type in dtype_dict.iteritems()}

    df = pd.read_csv(pip_result_csv, names=field_names, dtype=dtype_val)

    # If we're working with glad alerts, need to do some specific filtering
    # and need to sum by alerts
    if api_endpoint_object.forest_dataset == 'umd_landsat_alerts':
        print 'filtering CSV- regular glad'

        # filter valid confidence only
        df = df[(df['confidence'] == 2) | (df['confidence'] == 3)]

        if api_endpoint_object.contextual_dataset in ['wdpa', 'peat', 'moratorium']:

            df['month'] = df.apply(util.df_year_day_to_month, axis=1)
            groupby_list = ['country_iso', 'state_id', 'dist_id', 'year', 'month']

            if api_endpoint_object.contextual_dataset == 'wdpa':
                groupby_list += ['wdpa_id']

            df_groupby = df.groupby(groupby_list)['alerts',].sum().reset_index()
            final_record_list = df_groupby.to_dict(orient='records')

        else:
            # group by day and year, then sum
            groupby_list = ['country_iso', 'state_id', 'day', 'year', 'confidence']

            # df_groupby = df.groupby(groupby_list)['alerts', 'above_ground_carbon_loss', 'area_m2'].sum()
            df_groupby = df.groupby(groupby_list)['alerts', ].sum()

            # df -> list of records
            final_record_list = df_to_json.df_to_json(df_groupby)
            print 'finished filtering/groupby'

    # Custom process/filtering for climate data
    elif climate:
        print 'filtering CSV for climate'

        # filter: confirmed only
        print 'filtering to select where confidence == 3'
        df = df[df['confidence'] == 3]

        # filter: where climate_mask is 1 or where other countries exist
        # don't want to include RUS for climate stuff for now
        # MYS, IDN and COD not included-- must have climate_mask == 1 to be valid
        country_list = ['BRA', 'PER', 'COG', 'UGA', 'TLS', 'CMR', 'BDI',
                        'GAB', 'BRN', 'CAF', 'GNQ', 'PNG', 'SGP', 'RWA']

        print 'filtering to select country list, or where climate_mask == 1'
        df = df[(df['climate_mask'] == 1) | (df['country_iso'].isin(country_list))]

        # 1/1/2016 should be categorized as week 53 of 2015. This code creates that proper combination of
        # week# and year based on ISO calendar

        print 'calculating week and year for each date'
        df['week'], df['year'] = zip(*df.apply(lambda row: json_groupby_week.build_week_lookup(row['day'], row['year']), axis=1))

        # group by week and year, then sum
        print 'grouping by week and year, summing alerts, above_ground_carbon_loss and area_m2'
        groupby_list = ['country_iso', 'state_id', 'week', 'year', 'confidence', 'climate_mask']
        df_groupby = df.groupby(groupby_list)['alerts', 'above_ground_carbon_loss', 'area_m2'].sum()

        # df -> list of records, so we can run the cumulative values
        print 'sorting data frame to list of records'
        raw_record_list = df_to_json.df_to_json(df_groupby, climate)

        # cumulate values
        print 'cumulating values'
        cum_record_list = json_groupby_week.cum_values(raw_record_list)

        # fill in missing weeks data for glad 2016 only
        print 'adding dummy data'
        final_record_list = insert_dummyweeks.insert_dummy_cumulative_rows(cum_record_list)
        print 'finished filtering/groupby for climate'

    # Otherwise the output from hadoop_pip is already summarized for us, just need
    # to put it in [row, row, row, ...] format
    else:
        final_record_list = df.to_dict('records')

    if environment in ['prod', 'staging']:
        # write outputs to final file
        write_outputs(final_record_list, api_endpoint_object.s3_url)

    else:
        print 'Test run, not pushing JSON output to S3'

