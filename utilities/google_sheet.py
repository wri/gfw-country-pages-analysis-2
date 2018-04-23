import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=923735044
dataset_lookup_key = r'174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo'


def get_valid_inputs():

    forest_change_datasets = get_column_values('forest_change_iso_lookup', 'DATASET_TECHNICAL_NAME')
    contextual_datasets = get_column_values('contextual_iso_lookup', 'DATASET_TECHNICAL_NAME')

    unique_datasets = forest_change_datasets + contextual_datasets

    return unique_datasets


def get_column_values(sheet_name, col_name):

    gdoc_as_lists = get_all_gdoc_rows(sheet_name)

    col_index = gdoc_as_lists[0].index(col_name)
    data_rows = gdoc_as_lists[1:]

    col_values = [x[col_index] for x in data_rows]
    unique_values = list(set(col_values))

    return unique_values


def filter_gdoc_rows(sheet_name, column_name, column_value_list):

    output_list = []
    gdoc_as_lists = get_all_gdoc_rows(sheet_name)

    header_row = gdoc_as_lists[0]
    col_index = header_row.index(column_name)
    data_rows = gdoc_as_lists[1:]

    for row in data_rows:
        if row[col_index] in column_value_list:
            output_list.append(dict(zip(header_row, row)))
        else:
            pass

    return output_list


def get_all_gdoc_rows(sheet_name):

    wks = _open_spreadsheet(sheet_name)
    gdoc_as_lists = wks.get_all_values()

    return gdoc_as_lists


def get_associated_values(sheet_name, input_col_name, column_value_list, output_col_name):

    filtered_rows = filter_gdoc_rows(sheet_name, input_col_name, column_value_list)

    return [x[output_col_name] for x in filtered_rows]


def get_hadoop_config(input_dataset_name, environment):

    hadoop_config = None
    input_tuple = (input_dataset_name, environment)

    hadoop_rows = get_all_gdoc_rows('hadoop')
    header_row = hadoop_rows[0]

    for row in hadoop_rows[1:]:
        row_dict = dict(zip(header_row, row))

        row_tuple = (row_dict['forest_dataset'], row_dict['version'])

        if set(row_tuple) == set(input_tuple):
            root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config_dir = os.path.join(root_dir, 'config')

            hadoop_filename = row_dict['config_file']
            hadoop_config = os.path.join(config_dir, hadoop_filename)

    return hadoop_config


def get_sheet_name(input_tech_title, same_sheet=True):
    forest_change_datasets = get_column_values('forest_change_iso_lookup', 'DATASET_TECHNICAL_NAME')

    if input_tech_title in forest_change_datasets:
        if same_sheet:
            sheet_name = 'forest_change_iso_lookup'
        else:
            sheet_name = 'contextual_iso_lookup'

    else:
        if same_sheet:
            sheet_name = 'forest_change_iso_lookup'
        else:
            sheet_name = 'contextual_iso_lookup'

    return sheet_name


def _open_spreadsheet(sheet_name):
    """
    Open the spreadsheet for read/update
    :return: a gspread wks object that can be used to edit/update a given sheet
    """

    # Updated for oauth2client
    # http://gspread.readthedocs.org/en/latest/oauth2.html
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    spreadsheet_file = os.path.join(root_dir, 'tokens', 'spreadsheet.json')

    cred_list = ['https://spreadsheets.google.com/feeds']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(spreadsheet_file, cred_list)

    gc = gspread.authorize(credentials)
    wks = gc.open_by_key(dataset_lookup_key).worksheet(sheet_name)

    return wks


class SheetLayerDef(object):
    # Source: http://stackoverflow.com/a/1305663/4355916
    def __init__(self, **entries):
        self.__dict__.update(entries)


def get_api_endpoint(forest_change_dataset, environment):
    gdoc_data_rows = get_all_gdoc_rows('api_endpoint_lookup')

    header_row = gdoc_data_rows[0]

    api_endpoint_def = None

    # set up list to contain all rows we want to update
    summaries_to_create = []
    for row in gdoc_data_rows[1:]:

        row_dict = dict(zip(header_row, row))

        forest_match = row_dict['forest_dataset'] == forest_change_dataset
        version_match = row_dict['version'] == environment

        if forest_match and version_match:

            api_endpoint_def = SheetLayerDef(**row_dict)

            summaries_to_create.append(api_endpoint_def)

    if not api_endpoint_def:
        raise ValueError("No matching record in the google "
                         "sheet for datasets: {}, {} and version {}".format(dataset1, dataset2, environment))

    return summaries_to_create
