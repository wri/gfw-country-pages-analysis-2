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

        self.results_list = None
        self.associated_dataset_list = []
        self.update_api_dict = {}

    def get_associated_datasets(self, associated_dataset):

        self.associated_dataset_list = gs.get_associated_datasets(self.dataset_technical_name, associated_dataset)

    def calculate_summary_values(self):

        self.results_list = hadoop.pip(self.dataset_technical_name, self.associated_dataset_list)
        # self.results_list = [['gadm2_boundary', r"D:\scripts\gfw-country-pages-analysis-2\results\gadm2_boundary.csv"]]

        for associated_dataset_name, local_path_list in self.results_list:

            # Grab the outfile location on F:\ for the dataset/associated dataset/test combination
            cp_api_endpoint = gs.get_api_endpoint(self.dataset_technical_name, associated_dataset_name, self.is_test)

            # Process the hadoop CSV into JSON, and write the output
            output_json.output_json(local_path_list, cp_api_endpoint)

            # Add dataset ID and S3 URL of matching dataset to the update_api_dict
            self.update_api_dict[cp_api_endpoint.dataset_id] = cp_api_endpoint.web_url

            # If the datasets being processed are GLAD and gadm2, write climate output too
            if {self.dataset_technical_name, associated_dataset_name} == {'umd_landsat_alerts', 'gadm2_boundary'}:
                self.update_climate(local_path_list)

    def update_climate(self, local_path):
        climate_name = self.dataset_technical_name + '_climate'

        # Grab the outfile location on F:\ for the dataset/associated dataset/climate/test combination
        climate_api_endpoint = gs.get_api_endpoint(climate_name, 'gadm2_boundary', self.is_test)

        # Process the hadoop CSV into JSON, and write the output
        output_json.output_json(local_path, climate_api_endpoint, climate=True)

        # Add dataset ID and S3 URL of matching dataset to the update_api_dict
        self.update_api_dict[climate_api_endpoint.dataset_id] = climate_api_endpoint.web_url

    def push_to_gfw_api(self):

        for api_dataset_id, s3_url in self.update_api_dict.iteritems():

            print 'Pushing {0} to dataset ID {1} on the GFW country pages API'.format(s3_url, api_dataset_id)
            api.sync(api_dataset_id, s3_url)
