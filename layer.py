import pandas as pd
import json
import subprocess
import os, sys

from utilities import google_sheet as gs, hadoop, api, calculate_stats, util, climate, table_update


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
        self.result_csv = None
        self.update_api_dict = {}

    def calculate_summary_values(self):

        # run hadoop process to get table to summarize
        self.result_csv = hadoop.pip(self.dataset_technical_name, self.environment)

        cp_api_endpoint_objects_list = gs.get_api_endpoint(self.dataset_technical_name, self.environment)

        # Process the hadoop CSV into JSON, and write the output
        hadoop_output_df = util.hadoopresult_to_df(self.result_csv, self.dataset_technical_name)

        # iterate over types of summaries to create (iso, adm1, adm2, all)
        for cp_api_endpoint_object in cp_api_endpoint_objects_list:
            if cp_api_endpoint_object.summary_type == 'all':

                # make sure that bound1 and bound2 are strings
                hadoop_output_df.bound1 = hadoop_output_df.bound1.astype(str)
                hadoop_output_df.bound2 = hadoop_output_df.bound2.astype(str)
                hadoop_output_df = hadoop_output_df.sort_values('bound2', ascending=False)

                # copy to s3
                util.write_outputs(hadoop_output_df, cp_api_endpoint_object.s3_url, self.environment)
            else:
                self.process_table(hadoop_output_df, cp_api_endpoint_object)

            # Add dataset ID and S3 URL of matching dataset to the update_api_dict
            print "updating api dict with key: {0}, value: {1}".format(cp_api_endpoint_object.dataset_id,
                                                                       cp_api_endpoint_object.web_url)
            self.update_api_dict[cp_api_endpoint_object.dataset_id] = cp_api_endpoint_object.web_url
        print self.update_api_dict

    def push_to_gfw_api(self):

        for api_dataset_id, s3_url in self.update_api_dict.iteritems():
            print 'Pushing {0} to dataset ID {1} on the ' \
                  'GFW country pages API'.format(s3_url, api_dataset_id)
            api.sync(api_dataset_id, s3_url, self.environment)

    def process_table(self, df, api_endpoint_object):

        """
        Take the local output from the Hadoop PIP process and write a JSON file
        :param df: the local hadoop PIP CSV
        :param api_endpoint_object: a row from the config sheet:
         https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=923735044
        :return:
        """

        print 'processing update for {0}, summing by {1}'.format(api_endpoint_object.forest_dataset,
                                                                 api_endpoint_object.summary_type)

        # Custom process/filtering for climate data
        if api_endpoint_object.summary_type == 'climate':
            final_df = climate.climate_table_update(df, api_endpoint_object)

        elif api_endpoint_object.summary_type in ['iso', 'adm1', 'adm2']:
            if api_endpoint_object.forest_dataset == 'umd_landsat_alerts':
                final_df = table_update.glad_table_update(df, api_endpoint_object)

            elif api_endpoint_object.forest_dataset == 'fires':
                final_df = table_update.fires_table_update(df, api_endpoint_object)
            else:
                print "no valid forest type found" ## need to work on this exception


        else:
            final_df = df

        # write outputs to final file (only push to s3 if prod or staging)
        util.write_outputs(final_df, api_endpoint_object.s3_url, self.environment)
