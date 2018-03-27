import datetime

import pandas as pd



def cumsum(df):

    # create DF of all week/year combinations
    week_year_df = df.groupby(['week','year']).size().reset_index()
    week_year_df.columns = ['week', 'year', 'dummy_key']

    # create DF of all iso/adm1 combinations
    iso_adm1_df = df.groupby(['iso', 'adm1']).size().reset_index()
    iso_adm1_df.columns = ['iso', 'adm1', 'dummy_key']

    # make a dummy key for each so we can join them
    week_year_df['dummy_key'] = 1
    iso_adm1_df['dummy_key'] = 1

    # join them to get all possible week/year and state/iso combos
    # important for cumsum so even if there aren't GLAD alerts for a
    # particular week, we'll still have data
    dummy_df = pd.merge(iso_adm1_df, week_year_df, on='dummy_key')
    del dummy_df['dummy_key']

    # join our original DF to this dummy DF
    join_fields = ['iso', 'adm1', 'year', 'week']
    merged = pd.merge(dummy_df, df, on=join_fields, how='left')

    # fill loss and emissions data with 0 weeks without GLAD alerts
    merged.above_ground_carbon_loss.fillna(0, inplace=True)
    merged.loss_ha.fillna(0, inplace=True)

    # sort values for cumsum
    merged = merged.sort_values(join_fields)

    # remove week from join fields - want to cumsum using week
    join_fields.remove('week')

    # create cumsummed totals
    merged['cumulative_deforestation'] = merged.groupby(join_fields)['loss_ha'].cumsum().round(4)
    merged['cumulative_emissions'] = merged.groupby(join_fields)['above_ground_carbon_loss'].cumsum().round(4)

    merged = add_emissions_targets(merged)

    return format_df(merged)


def build_week_lookup(row):

    as_date = datetime.date(row['year'], 1, 1) + datetime.timedelta(row['julian_day'] - 1)
    year_num = as_date.isocalendar()[0]
    week_num = as_date.isocalendar()[1]

    return week_num, year_num


def format_df(df):

    # round emissions and loss area for display
    df.above_ground_carbon_loss = df.above_ground_carbon_loss.round(4)
    df.loss_ha = df.loss_ha.round(4)

    # add state_iso column to make it easier to query from the front end
    df['state_iso'] = df.iso + df.adm1.astype(str)

    # rename columns to fit regular climate schema
    df = df.rename(columns={'adm1': 'state_id', 'iso': 'country_iso'})

    return df


def add_emissions_targets(df):

    target_2020_list = [['IDN', 198.44, 521996.0],
                        ['COG', 9.202262991, 14594.16161],
                        ['BRA', 229.5, 965000.0],
                        ['PER', 23.46251872, 67526.67565],
                        ['UGA', 5.5, 18590.0],
                        ['BRN', 0.452, 1359.0],
                        ['TLS', 0.487, 1563.0],
                        ['CMR', 16.183, 48575.0],
                        ['MYS', 42.733, 122938.0],
                        ['COD', 46.484, 110488.0],
                        ['GAB', 7.472, 19412.0],
                        ['CAF', 13.53, 48080.0],
                        ['GNQ', 1.635, 4776.0],
                        ['PNG', 23.386, 62686.0],
                        ['BDI', 0.211, 1190.0],
                        ['RWA', 0.252, 1257.0],
                        ['SGP', 0.008, 42.0]]

    target_df = pd.DataFrame(target_2020_list)
    target_df.columns = ['iso', 'target_emissions', 'target_deforestation']

    # join target_df to df based on iso
    df = pd.merge(df, target_df, on='iso', how='left')

    df['percent_to_emissions_target'] = ((df.cumulative_emissions / df.target_emissions) * 100).round(4)
    df['percent_to_deforestation_target'] = ((df.cumulative_deforestation / df.target_deforestation) * 100).round(4)

    # remove joined columns
    del df['target_emissions'], df['target_deforestation']

    return df
