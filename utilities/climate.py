import pandas as pd
import log

def climate_table_update(df):
    df = df.copy()

    log.info("Selecting gadm28/admin polygons and confirmed alerts only")
    df = df[(df.polyname.isin(['gadm28', 'admin'])) & (df.confidence == 3)]

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

    log.info("Filtering to select country list, or where climate_mask == 1")
    df = df[(df['climate_mask'] == 1) | (df['iso'].isin(country_list))]

    # 1/1/2016 should be categorized as week 53 of 2015. This code creates that proper combination of
    # week# and year based on ISO calendar
    log.info("Calculating week and year for each date")
    df['week'] = df.alert_date.dt.week
    df['year'] = df.alert_date.dt.year

    # group by week and year, then sum
    log.info("Grouping by week and year, summing alerts, above_ground_carbon_loss and loss_ha")
    groupby_list = ['iso', 'adm1', 'week', 'year', 'confidence']
    sum_list = ['alerts', 'above_ground_carbon_loss', 'loss_ha']
    df_groupby = df.groupby(groupby_list)[sum_list].sum().reset_index()

    final_df = cumsum(df_groupby)

    return final_df


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

    # fill dummy weeks with appropriate N/A values
    merged.confidence.fillna('confirmed', inplace=True)
    merged.alerts.fillna(0, inplace=True)
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


def format_df(df):

    # round emissions and loss area for display
    df.above_ground_carbon_loss = df.above_ground_carbon_loss.round(4)
    df.loss_ha = df.loss_ha.round(4)

    # add state_iso column to make it easier to query from the front end
    df['state_iso'] = df.iso + df.adm1.astype(str)

    # rename columns to fit regular climate schema
    df = df.rename(columns={'adm1': 'state_id', 'iso': 'country_iso'})

    # sort DF so that elastic guesses each field type correctly
    df = df.sort_values('alerts', ascending=False)

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
                        ['RWA', 10.252, 1257.0],
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
