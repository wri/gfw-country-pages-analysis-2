import json


def move_key_value(df_dict, climate):

    final_list = []
    for key_tuple, value_dict in df_dict.iteritems():

        temp_dict = {}


        if climate:
            temp_dict['country_iso'] = key_tuple[0]
            temp_dict['state_iso'] = key_tuple[1]
            temp_dict['year'] = key_tuple[3]
            temp_dict['confidence'] = key_tuple[4]
            temp_dict['alerts'] = int(value_dict['alerts'])
            temp_dict['week'] = int(key_tuple[2])
            temp_dict['prf'] = key_tuple[5]

            temp_dict['above_ground_carbon_loss'] = round(value_dict['above_ground_carbon_loss'], 4)
            temp_dict['loss'] = round(float(value_dict['area_m2'])/10000, 4)

            final_list.append(temp_dict)
        else:
            temp_dict['country_iso'] = key_tuple[0]
            temp_dict['state_iso'] = key_tuple[1]
            temp_dict['year'] = key_tuple[3]
            temp_dict['confidence'] = key_tuple[4]
            temp_dict['day'] = int(key_tuple[2])

            temp_dict['alerts'] = int(value_dict['alerts'])

            final_list.append(temp_dict)

    return final_list


def df_to_json(data_frame, climate):

    df_transp = data_frame.transpose()

    # convert that back to a dictionary
    df_dict = df_transp.to_dict()

    # move our tuple key into the values part
    final_list = move_key_value(df_dict, climate)

    if climate:
        final_list_sorted = sorted(final_list, key=lambda k: (k['country_iso'], k['state_iso'], k['year'], k['week']))
    else:
        final_list_sorted = sorted(final_list, key=lambda k: (k['country_iso'], k['state_iso'], k['year'], k['day']))


    return final_list_sorted
