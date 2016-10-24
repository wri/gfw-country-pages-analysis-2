import pandas
import json
import os

from json_groupby_week import build_week_lookup
from df_to_json import df_to_json
from json_groupby_week import cum_values
from insert_dummyweeks import insert_dummy_cumulative_rows


def write_outputs(values_dict, output_json_file):

    # add list of dictionaries as value for "data" key
    final_dict = {"data": values_dict}

    # write to json
    with open(output_json_file, 'w') as outfile:
        json.dump(final_dict, outfile)

    # add csv file to output
    csv_results = os.path.splitext(output_json_file)[0] + '.csv'

    # write to csv
    df = pandas.DataFrame(values_dict)
    df.to_csv(csv_results)


def output_json(pip_result_csv, output_json_file, climate=False):

    # csv to pandas data frame
    field_names = ['confidence', 'year', 'day', 'area_m2', 'above_ground_carbon_loss', 'prf',
                   'country_iso', 'state_iso', 'dist_iso', 'alerts']
    pandas_csv = pandas.read_csv(pip_result_csv, names=field_names)
    df = pandas.DataFrame(pandas_csv)

    if climate:
        # filter: confirmed only
        df = df[df['confidence'] == 3]

        # filter: where prf is 1 or where other countries exist
        # don't want to include RUS for climate stuff for now
        df = df[(df['prf'] == 1) | (df['country_iso'].isin(['BRA', 'PER', 'COG', 'UGA']))]

        # calculate week number
        df['week'] = df.apply(lambda x: build_week_lookup(x['day'], x['year']), axis=1)

        # if week number is 53, then set year to 2015
        df.ix[df.week == 53, 'year'] = 2015

        # group by week and year, then sum
        groupby_list = ['country_iso', 'state_iso', 'week', 'year', 'confidence', 'prf']
        df_groupby = df.groupby(groupby_list)['alerts', 'above_ground_carbon_loss', 'area_m2'].sum()

        # df -> list of records, so we can run the cumulative values
        raw_record_list = df_to_json(df_groupby, climate)

        # cumulate values
        cum_record_list = cum_values(raw_record_list)

        # fill in missing weeks data for glad 2016 only
        final_record_list = insert_dummy_cumulative_rows(cum_record_list)

    else:
        # filter valid confidence only
        df = df[(df['confidence'] == 2) | (df['confidence'] == 3)]

        # group by day and year, then sum
        groupby_list = ['country_iso', 'state_iso', 'day', 'year', 'confidence']
        df_groupby = df.groupby(groupby_list)['alerts', 'above_ground_carbon_loss', 'area_m2'].sum()

        # df -> dict, so we can run the cumulative values
        final_record_list = df_to_json(df_groupby, climate)

    # write climate outputs to final file
    write_outputs(final_record_list, output_json_file)
