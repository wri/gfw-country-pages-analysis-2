import shutil

from gfw_country_pages_analysis_2.utilities import (
    google_sheet as gs,
    hadoop,
    api,
    util,
    climate,
    table_update,
    log,
)


class Layer(object):
    """ A general Layer class. Used to pull information from the google sheet config table and pass it to
    various layer update function
    :return:
    """

    def __init__(self, dataset_technical_name, environment):
        log.info("Starting layer class")

        self.dataset_technical_name = dataset_technical_name
        self.environment = environment

        # we'll download the hadoop results to a temp dir on our local machine
        self.result_csv = None
        self.temp_directory = None

        # after we postprocess our outputs and upload the final tables to S3,
        # we'll make a dict of {dataset_id: S3_csv} to update the API data
        self.update_api_dict = {}

        self._initialize()

    def calculate_summary_values(self):

        # run hadoop process to get table to summarize
        self.result_csv, self.temp_directory = hadoop.pip(
            self.dataset_technical_name, self.environment
        )

        cp_api_endpoint_objects_list = gs.get_api_endpoint(
            self.dataset_technical_name, self.environment
        )

        # Process the hadoop CSV into JSON, and write the output
        hadoop_output_df = util.hadoopresult_to_df(
            self.result_csv, self.dataset_technical_name
        )

        # iterate over types of summaries to create (iso, adm1, adm2, all)
        for cp_api_endpoint_object in cp_api_endpoint_objects_list:
            if cp_api_endpoint_object.summary_type == "all":

                # convert from datetime to date so we can serialize easily
                hadoop_output_df.alert_date = hadoop_output_df.alert_date.dt.date

                # make sure that bound1 and bound2 are strings
                hadoop_output_df.bound1 = hadoop_output_df.bound1.astype(str)
                hadoop_output_df.bound2 = hadoop_output_df.bound2.astype(str)
                hadoop_output_df = hadoop_output_df.sort_values(
                    "bound2", ascending=False
                )

                # if it's GLAD and all, need to drop a bunch more fields
                # we don't care about all the polyname stuff
                if cp_api_endpoint_object.forest_dataset == "umd_landsat_alerts":
                    hadoop_output_df = table_update.glad_all_update(hadoop_output_df)

                # copy to s3
                util.write_outputs(
                    hadoop_output_df, cp_api_endpoint_object.s3_url, self.environment
                )

            else:
                self.process_table(hadoop_output_df, cp_api_endpoint_object)

            # Add dataset ID and S3 URL of matching dataset to the update_api_dict
            log.info(
                "updating api dict with key: {0}, value: {1}".format(
                    cp_api_endpoint_object.dataset_id, cp_api_endpoint_object.web_url
                )
            )

            self.update_api_dict[
                cp_api_endpoint_object.dataset_id
            ] = cp_api_endpoint_object.web_url

        log.info(self.update_api_dict)

    def push_to_gfw_api(self):

        for api_dataset_id, s3_url in self.update_api_dict.iteritems():
            log.info(
                "Pushing {0} to dataset ID {1} on the "
                "GFW country pages API".format(s3_url, api_dataset_id)
            )
            api.sync(api_dataset_id, s3_url, self.environment)

    def process_table(self, df, api_endpoint_object):

        """
        Take the local output from the Hadoop PIP process and write a JSON file
        :param df: the local hadoop PIP CSV
        :param api_endpoint_object: a row from the config sheet:
         https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=923735044
        :return:
        """

        log.info(
            "Processing update for {0}, summing by {1}".format(
                api_endpoint_object.forest_dataset, api_endpoint_object.summary_type
            )
        )

        # Custom process/filtering for climate data
        if api_endpoint_object.summary_type == "climate":
            final_df = climate.climate_table_update(df)

        elif api_endpoint_object.summary_type in ["iso", "adm1", "adm2"]:

            if api_endpoint_object.forest_dataset == "umd_landsat_alerts":
                final_df = table_update.glad_table_update(df, api_endpoint_object)

            elif api_endpoint_object.forest_dataset in [
                "fires_report",
                "fires_country_pages",
            ]:
                final_df = table_update.fires_table_update(df, api_endpoint_object)

            else:
                log.warning(
                    "No valid forest type found"
                )  # need to work on this exception

        else:
            final_df = df

        # write outputs to final file (only push to s3 if prod or staging)
        util.write_outputs(final_df, api_endpoint_object.s3_url, self.environment)

    def remove_temp_output_dir(self):

        # clean up our temp directories to save machine space,
        # particularly on the GFW staging server
        if self.temp_directory:
            shutil.rmtree(self.temp_directory)

    def finalize(self, failed=False):
        pass

    def _initialize(self):
        pass


class GladLayer(Layer):
    # TODO: This is just a basic sub type. There are several other methods that we still need to pull out.

    def finalize(self, failed=False):

        if self.environment != "prod":
            test = True
        else:
            test = False

        if failed:
            status = "HADOOP FAILED"
        else:
            status = "COMPLETED"

        util.update_status(status, test)

    def _initialize(self):
        test = False
        if self.environment != "prod":
            test = True
        util.update_status("HADOOP RUNNING", test)
