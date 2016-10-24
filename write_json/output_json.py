import pandas
import json
import os

from json_groupby_week import build_week_lookup
from df_to_json import df_to_json
from json_groupby_week import cum_values
from insert_dummyweeks import insert_dummy_cumulative_rows


def write_outputs(values_dict, results_dir, climate):
    if climate:
        outname = 'climate'
    else:
        outname = 'country_pages'

    # add list of dictionaries as value for "data" key
    final_dict = {"data": values_dict}

    # name files
    json_results = os.path.join(results_dir, 'outputs_{}.json'.format(outname))
    csv_results = os.path.join(results_dir, 'outputs_{}.csv'.format(outname))

    # write to json
    with open(json_results, 'w') as outfile:
        json.dump(final_dict, outfile)

    # write to csv
    df = pandas.DataFrame(values_dict)
    df.to_csv(csv_results)


def output_json(pip_outputs, climate=False):

    this_dir = os.path.dirname(os.path.abspath(__file__))
    main_dir = os.path.dirname(this_dir)
    results_dir = os.path.join(main_dir, "results")

    # csv to pandas data frame
    field_names = ['confidence', 'year', 'day', 'area_m2', 'above_ground_carbon_loss', 'prf',
                   'country_iso', 'state_iso', 'dist_iso', 'alerts']
    pandas_csv = pandas.read_csv(pip_outputs, names=field_names)
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
        df_groupby = df.groupby(['country_iso', 'state_iso', 'week', 'year', 'confidence', 'prf'])[
            'alerts', 'above_ground_carbon_loss', 'area_m2'].sum()

        # df -> list of records, so we can run the cumulative values
        record_list = df_to_json(df_groupby, climate)

        # cumulate values
        cum_record_list = cum_values(record_list)

        # fill in missing weeks data for glad 2016 only
        record_list_w_dummy_weeks = insert_dummy_cumulative_rows(cum_record_list)

        # write climate outputs to final file
        write_outputs(record_list_w_dummy_weeks, results_dir, climate)

    else:
        # filter valid confidence only
        df = df[(df['confidence'] == 2) | (df['confidence'] == 3)]

        # group by day and year, then sum
        df_groupby = df.groupby(['country_iso', 'state_iso', 'day', 'year', 'confidence'])[
            'alerts', 'above_ground_carbon_loss', 'area_m2'].sum()

        # df -> dict, so we can run the cumulative values
        values_dict = df_to_json(df_groupby, climate)

        # write climate outputs to final file
        write_outputs(values_dict, results_dir, climate)
