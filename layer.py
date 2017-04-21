from utilities import google_sheet as gs, hadoop, api
from write_json import output_json


class Layer(object):
    """ A general Layer class. Used to pull information from the google sheet config table and pass it to
    various layer update function
    :return:
    """

    def __init__(self, dataset_technical_name, environment):
        print 'Starting layer class'

        self.dataset_technical_name = dataset_technical_name
        self.environment = environment

        self.results_list = None
        self.associated_dataset_list = []
        self.update_api_dict = {}

    def get_associated_datasets(self, associated_dataset):

        all_datasets = gs.get_associated_datasets(self.dataset_technical_name, associated_dataset)
        self.associated_dataset_list = list(set(all_datasets))

    def calculate_summary_values(self):

        self.results_list = hadoop.pip(self.dataset_technical_name, self.associated_dataset_list, self.environment)

        for associated_dataset_name, local_path_list in self.results_list:

            # Grab the outfile location on F:\ for the dataset/associated dataset/test combination
            cp_api_endpoint = gs.get_api_endpoint(self.dataset_technical_name, associated_dataset_name, self.environment)

            # Process the hadoop CSV into JSON, and write the output
            output_json.output_json(local_path_list, cp_api_endpoint, self.environment)

            # Add dataset ID and S3 URL of matching dataset to the update_api_dict
            self.update_api_dict[cp_api_endpoint.dataset_id] = cp_api_endpoint.web_url

            # make a set of the technical name + associated dataset. we never know what order
            # they'll come in (gadm2 could be dataset, and GLAD could be associated, for example
            input_set = {self.dataset_technical_name, associated_dataset_name}

            # If the datasets being processed are GLAD and gadm2, write climate output too
            if input_set == {'umd_landsat_alerts', 'gadm2_boundary'}:
                self.update_additional('climate', associated_dataset_name, local_path_list)

                self.update_additional('month', associated_dataset_name, local_path_list)

            if input_set == {'terra_i_alerts', 'gadm1_boundary'}:
                self.update_additional('month', associated_dataset_name, local_path_list)

    def update_additional(self, update_name, associated_dataset_name, local_path):

        update_dataset_name = self.dataset_technical_name + '_' + update_name

        # Grab the outfile location on F:\ for the dataset/associated dataset/climate/test combination
        update_api_endpoint = gs.get_api_endpoint(update_dataset_name, associated_dataset_name, self.environment)

        # Process the hadoop CSV into JSON, and write the output
        output_json.output_json(local_path, update_api_endpoint, self.environment, update_name=update_name)

        # Add dataset ID and S3 URL of matching dataset to the update_api_dict
        self.update_api_dict[update_api_endpoint.dataset_id] = update_api_endpoint.web_url

    def push_to_gfw_api(self):

        if self.environment in ['prod', 'staging']:

            for api_dataset_id, s3_url in self.update_api_dict.iteritems():

                print 'Pushing {0} to dataset ID {1} on the GFW country pages API'.format(s3_url, api_dataset_id)
                api.sync(api_dataset_id, s3_url, self.environment)

        else:
            print 'Currently running this as test; not pushing anything to gfw API'
