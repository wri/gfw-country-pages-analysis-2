

def move_key_value(df_dict, climate):

    final_list = []

    conf_values = {2: "unconfirmed", 3: "confirmed"}

    for key_tuple, value_dict in df_dict.iteritems():

        temp_dict = {}

        temp_dict['country_iso'] = key_tuple[0]
        temp_dict['state_iso'] = key_tuple[1]
        temp_dict['year'] = key_tuple[3]
        temp_dict['confidence'] = conf_values[key_tuple[4]]
        temp_dict['alerts'] = int(value_dict['alerts'])

        if climate:
            temp_dict['week'] = int(key_tuple[2])
            temp_dict['above_ground_carbon_loss'] = round(value_dict['above_ground_carbon_loss'], 4)
            temp_dict['loss_ha'] = round(float(value_dict['area_m2'])/10000, 4)

        else:
            temp_dict['day'] = int(key_tuple[2])

        final_list.append(temp_dict)

    return final_list


def df_to_json(data_frame, climate):

    df_transp = data_frame.transpose()

    # convert that back to a dictionary
    df_dict = df_transp.to_dict()

    # move our tuple key into the values part
    final_list = move_key_value(df_dict, climate)

    row_keys = ['country_iso', 'state_iso', 'year']

    if climate:
        row_keys.append('week')
    else:
        row_keys.append('day')

    final_list_sorted = sorted(final_list, key=lambda k: ([k[key] for key in row_keys]))

    return final_list_sorted
