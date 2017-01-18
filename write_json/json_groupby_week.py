
import datetime


def build_week_lookup(day_num, year):

    as_date = datetime.date(year, 1, 1) + datetime.timedelta(day_num - 1)
    year_num = as_date.isocalendar()[0]
    week_num = as_date.isocalendar()[1]

    return week_num, year_num


def cum_values(climate_dict):

    target_2020_dict = {'IDN': {'emiss_t': 41.04496782, 'defor_t': 100815.0},
                        'COG': {'emiss_t': 9.202262991, 'defor_t': 14594.16161},
                        'BRA': {'emiss_t': 229.5, 'defor_t': 965000.0},
                        'PER': {'emiss_t': 23.46251872, 'defor_t': 67526.67565},
                        'UGA': {'emiss_t': 5.5, 'defor_t': 18590},
                        ##### DUMMY VALUES BELOW--  NEED TO FILL THESE IN WITH REAL DATA BEFORE LAUNCH
                        ##### Also need to fix IDN value-- currently for just kalimantan
                        'BRN': {'emiss_t': 5.5, 'defor_t': 18590},
                        'TLS': {'emiss_t': 5.5, 'defor_t': 18590},
                        'CMR': {'emiss_t': 5.5, 'defor_t': 18590},
                        'MYS': {'emiss_t': 5.5, 'defor_t': 18590},
                        'COD': {'emiss_t': 5.5, 'defor_t': 18590},
                        'GAB': {'emiss_t': 5.5, 'defor_t': 18590},
                        'CAF': {'emiss_t': 5.5, 'defor_t': 18590},
                        'GNQ': {'emiss_t': 5.5, 'defor_t': 18590},
                        'PNG': {'emiss_t': 5.5, 'defor_t': 18590},
                        'SGP': {'emiss_t': 5.5, 'defor_t': 18590}}

    for loss_emiss_dict in climate_dict:

        # for each dictionary in the list, get all dictionaries for the state and year
        filtered_state = list(filter(lambda s: s['state_id'] == loss_emiss_dict['state_id']
                                               and s['country_iso'] == loss_emiss_dict['country_iso']
                                               and s['year'] == loss_emiss_dict['year'], climate_dict))

        # then get all states less than the current week in the for loop
        filtered_state_week = list(filter(lambda w: w['week'] <= loss_emiss_dict['week'], filtered_state))

        cumm_loss = sum(d['loss_ha'] for d in filtered_state_week)
        cumm_emiss = sum(d['above_ground_carbon_loss'] for d in filtered_state_week)

        # create a key for cumulative loss and carbon loss and populate it with value
        loss_emiss_dict['cumulative_deforestation'] = cumm_loss
        loss_emiss_dict['cumulative_emissions'] = round(cumm_emiss, 4)

        # set variables for country iso, and 2020 targets
        country_iso = loss_emiss_dict['country_iso']
        emiss_target = target_2020_dict[country_iso]['emiss_t']
        defor_target = target_2020_dict[country_iso]['defor_t']

        # calculate values for targets
        loss_emiss_dict['percent_to_emissions_target'] = round(loss_emiss_dict['cumulative_emissions']/emiss_target * 100, 4)
        loss_emiss_dict['percent_to_deforestation_target'] = round(loss_emiss_dict['cumulative_deforestation']/defor_target * 100, 4)

    return climate_dict
