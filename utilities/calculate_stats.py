import pandas as pd
import json
import subprocess
import os

from utilities import util, climate


def write_outputs(results_df, output_s3_path, environment):

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    results_dir = os.path.join(root_dir, 'results')

    basename = os.path.basename(output_s3_path)
    results_csv = os.path.join(results_dir, basename)

    # write to csv
    results_df.to_csv(results_csv, index=False)

    # push to aws
    if environment in ['prod', 'staging']:
        cmd = ['aws', 's3', 'cp', results_csv, output_s3_path]
        subprocess.check_call(cmd)


def process_table(pip_result_csv, api_endpoint_object, environment, update_name=None):
    """
    Take the local output from the Hadoop PIP process and write a JSON file
    :param pip_result_csv: the local hadoop PIP CSV
    :param api_endpoint_object: a row from the config sheet:
     https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=923735044
    :param environment: used to designate if this is a staging run or not-- if so will include more countries in climate
    :param update_name: special updates (climate, monthly) to run
    :return:
    """
    print 'starting read CSV'

    # csv to pandas data frame
    field_names = api_endpoint_object.csv_field_names.split(',')

    # grab any custom datatype specifications from the API spreadsheet
    try:
        dtype_dict = json.loads(api_endpoint_object.dtypes)

        # convert them to format {'col_name': <python type>}
        dtype_val = {col_name: eval(col_type) for col_name, col_type in dtype_dict.iteritems()}

    # if empty cell in google sheet (represented as ''), no dytpe val supplied
    except ValueError:
        dtype_val = None

    df = pd.read_csv(pip_result_csv, names=field_names, dtype=dtype_val)

    # fill blank bounds - can cause issues if we groupby null values
    df.bound1 = df.bound1.fillna(1)
    df.bound2 = df.bound2.fillna(1)

    # If we're working with glad alerts, need to do some specific filtering
    # and need to sum by alerts
    if api_endpoint_object.forest_dataset == 'umd_landsat_alerts':

        # filter valid confidence only
        df = df[(df.confidence == 2) | (df.confidence == 3)]

        # group by day and year, then sum
        groupby_list = ['confidence', 'year', 'julian_day','polyname',
                        'bound1', 'bound2', 'iso', 'adm1', 'adm2']
        sum_list = ['alerts', 'above_ground_carbon_loss', 'area_m2']
        final_df = df.groupby(groupby_list)[sum_list].sum().reset_index()

        # convert area to ha from m2
        final_df['area_ha'] = final_df.area_m2 / 10000

        # convert year + day to date
        final_df['alert_date'] = final_df.apply(lambda row: util.to_jd(row), axis=1)
        del final_df['year'], final_df['julian_day'], final_df['area_m2']

        # create date string that elastic can recognize
        final_df.alert_date = final_df.alert_date.dt.strftime('%Y/%m/%d')

        # sort so elastic gets bound1/bound2 field types right
        final_df = final_df.sort_values('bound2', ascending=False)

    # Custom process/filtering for climate data
    elif update_name == 'climate':
        print 'filtering CSV for climate'

        print 'selecting gadm28 polygons and confirmed alerts only'
        df = df[(df.polyname == 'gadm28') & (df.confidence == 3)]

        # change confidence to user-friendly label (not '3')
        df['confidence'] = 'confirmed'

        # convert loss in area_m2 to ha
        df['loss_ha'] = df.area_m2 / 10000
        del df['area_m2']

        # filter: where climate_mask is 1 or where other countries exist
        # don't want to include RUS for climate stuff for now
        # MYS, IDN and COD not included-- must have climate_mask == 1 to be valid
        country_list = ['BRA', 'PER', 'COG', 'UGA', 'TLS', 'CMR', 'BDI',
                        'GAB', 'BRN', 'CAF', 'GNQ', 'PNG', 'SGP', 'RWA']

        print 'filtering to select country list, or where climate_mask == 1'
        df = df[(df['climate_mask'] == 1) | (df['iso'].isin(country_list))]

        # 1/1/2016 should be categorized as week 53 of 2015. This code creates that proper combination of
        # week# and year based on ISO calendar
        print 'calculating week and year for each date'
        df['week'], df['year'] = zip(*df.apply(lambda row: climate.build_week_lookup(row), axis=1))

        # group by week and year, then sum
        print 'grouping by week and year, summing alerts, above_ground_carbon_loss and loss_ha'
        groupby_list = ['iso', 'adm1', 'week', 'year', 'confidence']
        sum_list = ['alerts', 'above_ground_carbon_loss', 'loss_ha']
        df_groupby = df.groupby(groupby_list)[sum_list].sum().reset_index()

        final_df = climate.cumsum(df_groupby)

    else:
        final_df = df

    # write outputs to final file (only push to s3 if prod or staging)
    write_outputs(final_df, api_endpoint_object.s3_url, environment)
