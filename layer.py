from utilities import google_sheet as gs, hadoop, api, calculate_stats


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
        self.update_api_dict = {}

    def calculate_summary_values(self):

        # run hadoop process to get table to summarize
        self.result_csv = hadoop.pip(self.dataset_technical_name, self.environment)

        print 'processing {}'.format(self.dataset_technical_name)
        cp_api_endpoint = gs.get_api_endpoint(self.dataset_technical_name, self.environment)

        # Process the hadoop CSV into JSON, and write the output
        calculate_stats.process_table(self.result_csv, cp_api_endpoint, self.environment)

        # Add dataset ID and S3 URL of matching dataset to the update_api_dict
        self.update_api_dict[cp_api_endpoint.dataset_id] = cp_api_endpoint.web_url

        if self.dataset_technical_name == 'umd_landsat_alerts':
            self.update_additional('climate')

    def update_additional(self, update_name):

        update_dataset_name = self.dataset_technical_name + '_' + update_name
        print 'doing additional {} update for dataset {}'.format(update_name, self.dataset_technical_name)

        # Grab the outfile location on F:\ for the dataset/associated dataset/climate/test combination
        update_api_endpoint = gs.get_api_endpoint(update_dataset_name, self.environment)

        # Process the hadoop CSV into JSON, and write the output
        calculate_stats.process_table(self.result_csv, update_api_endpoint,
                                      self.environment, update_name=update_name)

        # Add dataset ID and S3 URL of matching dataset to the update_api_dict
        self.update_api_dict[update_api_endpoint.dataset_id] = update_api_endpoint.web_url

    def push_to_gfw_api(self):

        for api_dataset_id, s3_url in self.update_api_dict.iteritems():

            print 'Pushing {0} to dataset ID {1} on the ' \
                  'GFW country pages API'.format(s3_url, api_dataset_id)
            api.sync(api_dataset_id, s3_url, self.environment)
