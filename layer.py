from utilities import google_sheet as gs, hadoop, api
from write_json import output_json


class Layer(object):
    """ A general Layer class. Used to pull information from the google sheet config table and pass it to
    various layer update function
    :return:
    """

    def __init__(self, dataset_technical_name, is_test):
        print 'Starting layer class'

        self.dataset_technical_name = dataset_technical_name
        self.is_test = is_test

        self.dataset_country_dict = {}
        self.results = None
        self.update_api_dict = None

    def get_associated_datasets(self, associated_dataset):

        self.dataset_country_dict = gs.get_associated_datasets(self.dataset_technical_name, associated_dataset)

        print self.dataset_country_dict

    def calculate_summary_values(self):

        # self.results = hadoop.pip(self.dataset_technical_name, self.dataset_country_dict)
        self.results = [['gadm2_boundary', r"C:\Users\charlie.hofmann\Desktop\gadm2_boundary.csv"]]

        for associated_dataset_name, local_path in self.results:

            country_page_output = output_json.output_json(local_path)

            if self.dataset_technical_name == 'umd_landsat_alerts':
                climate_output = output_json.output_json(local_path, climate=True)
            else:
                climate_output = None



    def write_results_to_s3(self):

        print 'We have results!'


    def push_to_gfw_api(self):

        print 'pushing to API!'

        # for api_dataset_id, s3_url in self.update_api_dict.iteritems():
        #     print 'Pushing {0} to dataset ID {1} on the GFW country pages API'.format(s3_url, api_dataset_id)
        #
        #     api.sync(api_dataset_id, s3_url)