import datetime

import pandas as pd


def fires_table_update(df, api_endpoint_object):
    df = df.copy()

    # make sure that bound1 and bound2 are strings
    df.bound1 = df.bound1.astype(str)
    df.bound2 = df.bound2.astype(str)

    # convert string date to dt object
    print 'Converting date string to object'

    df.alert_date = pd.to_datetime(df.alert_date, infer_datetime_format=True, format='%Y/%m/%d')
    df.alert_date = df.alert_date.dt.date

    df['week'], df['year'] = zip(*df.apply(lambda row: build_week_lookup(row), axis=1))

    del df['alert_date']

    return group_and_to_csv(df, api_endpoint_object)

def glad_table_update(df, api_endpoint_object):

    df = df.copy()

    df = df[(df.confidence == 2) | (df.confidence == 3)]

    # make sure that bound1 and bound2 are strings
    df.bound1 = df.bound1.astype(str)
    df.bound2 = df.bound2.astype(str)

    # convert area to ha from m2
    df['area_ha'] = df.area_m2 / 10000

    # filter to remove stray countries
    # not totally important, but will cut down on the # of rows,
    # especially with the dummy rows we're adding
    valid_iso_list = ['BDI', 'BRA', 'BRN', 'CAF', 'CMR', 'COD', 'COG', 'COL',
                      'ECU', 'GAB', 'GNQ', 'GUF', 'GUY', 'IDN', 'MYS', 'PER',
                      'PNG', 'RWA', 'SUR', 'TLS', 'UGA', 'VEN']

    df = df[df.iso.isin(valid_iso_list)]

    # create column "alert_date from julian_day and year"
    df['alert_date'] = df.apply(lambda row: julian_date_from_yrday(row), axis=1)

    # drop columns that we don't want to aggregate by
    df.drop(['area_m2', 'julian_day', 'climate_mask', 'confidence'], axis=1, inplace=True)

    # convert string date to dt object
    print 'Converting date string to object'

    df.alert_date = pd.to_datetime(df.alert_date, infer_datetime_format=True, format='%Y/%m/%d')
    df.alert_date = df.alert_date.dt.date

    # figure out week and year based on isocalendar
    df['week'], df['year'] = zip(*df.apply(lambda row: build_week_lookup(row), axis=1))

    del df['alert_date']

    df_with_missing_weeks = add_missing_week_year(df)
    final_df = group_and_to_csv(df_with_missing_weeks, api_endpoint_object)

    return final_df


def add_missing_week_year(df):

    # group DF by week and year, then by polyname to get unique
    # lists of both
    year_week_df = df.groupby(['week', 'year']).size().reset_index()
    del year_week_df[0]

    adm_list = ['polyname', 'bound1', 'bound2', 'iso', 'adm1', 'adm2']
    polyname_df = df.groupby(adm_list).size().reset_index()
    del polyname_df[0]

    # add dummy join column to both, then join on it
    year_week_df['join_col'] = 1
    polyname_df['join_col'] = 1

    all_week_polyname_df = pd.merge(year_week_df, polyname_df, on='join_col')
    del all_week_polyname_df['join_col']

    # join df to this combination dataframe, so we can identify the
    # iso/adm1/adm2/polyname combinations that are missing weeks
    join_list = adm_list + ['week', 'year']
    joined_df = pd.merge(all_week_polyname_df, df, on=join_list, how='left')

    for sum_col in ['alerts', 'above_ground_carbon_loss', 'area_ha']:
        joined_df[sum_col].fillna(0, inplace=True)

    return joined_df


def group_and_to_csv(df, api_endpoint_object):

    group_list = df.columns.tolist()

    if api_endpoint_object.forest_dataset == 'fires':

        group_list.remove('alerts')
        sum_list = ['alerts']

    else:
        sum_list = ['above_ground_carbon_loss', 'alerts', 'area_ha']

    # exclude these columns from our groupby
    # if we're grouping at level 0, we want to exclude adm1 and adm2, etc

    level_lkp = {'iso': 0, 'adm1': 1, 'adm2': 2}
    adm_level_int = level_lkp[api_endpoint_object.summary_type]

    adm_lkp = {0: 'iso', 1: 'adm1', 2: 'adm2'}

    exclude_list = [adm_lkp[x] for x in range(adm_level_int + 1, 3)]

    exclude_list.extend(sum_list)

    group_list = [x for x in group_list if x not in exclude_list]
    grouped = df.groupby(group_list)[sum_list].sum().reset_index()

    # sort so elastic gets bound1/bound2 field types right
    grouped = grouped.sort_values('bound2', ascending=False)

    return grouped


def build_week_lookup(row):
    year_num, week_num, _ = row['alert_date'].isocalendar()
    return week_num, year_num


def julian_date_from_yrday(row):
    alert_year = row['year']
    j_day = row['julian_day']
    alert_date = (datetime.datetime(alert_year, 1, 1) + datetime.timedelta(j_day)).strftime('%Y%m%d')

    return alert_date
