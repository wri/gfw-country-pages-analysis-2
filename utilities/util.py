import os
import json
import datetime
import pandas as pd
import subprocess

def load_json_from_token(file_name):

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    token_file = os.path.join(root_dir, 'tokens', file_name)

    with open(token_file) as data_file:
        data = json.load(data_file)

    return data


def df_year_day_to_month(df):
    as_date = datetime.datetime.strptime(str(df.year) + str(df.day), '%Y%j')

    return as_date.month


def to_jd(row):
     return datetime.datetime(row['year'], 1, 1) + datetime.timedelta(row['julian_day'] - 1)


def build_week_lookup(row):

    as_date = datetime.date(row['year'], 1, 1) + datetime.timedelta(row['julian_day'] - 1)
    year_num = as_date.isocalendar()[0]
    week_num = as_date.isocalendar()[1]

    return week_num, year_num


def julian_date_from_yrday(row):
    alert_year = row['year']
    j_day = row['julian_day']
    alert_date = (datetime.datetime(alert_year, 1, 1) + datetime.timedelta(j_day)).strftime('%Y%m%d')

    return alert_date


def hadoopresult_to_df(result_csv, dataset_tech_name):

    terra_i_alerts_fields =  ['year', 'day', 'polyname', 'bound1', 'bound2', 'iso', 'adm1', 'adm2', 'alerts']

    umd_landsat_alerts_fields = ['confidence', 'year', 'julian_day', 'area_m2', 'above_ground_carbon_loss',
                                'climate_mask', 'polyname', 'bound1', 'bound2', 'iso', 'adm1', 'adm2', 'alerts']

    fire_alerts_fields = ['alert_date', 'fire_type', 'polyname', 'bound1', 'bound2', 'iso', 'adm1', 'adm2', 'alerts']

    analysis_fields_dict = {'terra_i_alerts': terra_i_alerts_fields,
                            'umd_landsat_alerts': umd_landsat_alerts_fields,
                            'fires': fire_alerts_fields}

    # read in field names
    field_names = analysis_fields_dict[dataset_tech_name]

    # csv to pandas data frame
    df = pd.read_csv(result_csv, names=field_names)

    # convert string date to dt object
    print 'Converting date string to object'
    df.alert_date = pd.to_datetime(df.alert_date, infer_datetime_format=True, format='%Y/%m/%d')
    df.alert_date = df.alert_date.dt.date

    # fill blank bounds - can cause issues if we groupby null values
    df.bound1 = df.bound1.fillna(1)
    df.bound2 = df.bound2.fillna(1)

    return df

def write_outputs(results_df, output_s3_path, environment):
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    results_dir = os.path.join(root_dir, 'results')

    basename = os.path.basename(output_s3_path)
    print "output s3 path: {}".format(output_s3_path)

    results_csv = os.path.join(results_dir, basename)

    # write to csv
    results_df.to_csv(results_csv, index=False)

    # push to aws
    if environment in ['prod', 'staging']:
        cmd = ['aws', 's3', 'cp', results_csv, output_s3_path]
        subprocess.check_call(cmd)
