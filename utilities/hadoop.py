import os
import sys
import subprocess

import google_sheet as gs

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
external_dir = os.path.dirname(root_dir)

sys.path.append(external_dir)

# The hadoop_pip package must be in the external dir referenced above, i.e.:
# - Desktop
#    - gfw-country-pages-analysis-2
#    - hadoop-pip

from hadoop_pip import run_pip


def pip(dataset_technical_name, associated_dataset_list):

    hadoop_config_list = []

    for associated_dataset_name in associated_dataset_list:

        config = gs.get_hadoop_config(dataset_technical_name, associated_dataset_name)
        hadoop_config_list.append(config)

    s3_result_list = run_pip.run(hadoop_config_list)

    # Response from hadoop comes back as list of lists
    # Example: [[s3://gfw2-data/alerts-tsv/hadoop-jobs/bb858284-8c4d-4e00-8473-69cef650a7f3/output1.csv",
    # r"r"s3://gfw2-data/alerts-tsv/peru_export.csv"]]

    # We may have submitted multiple jobs, but for each we only want the first item in the list
    s3_result_list = [x[0] for x in s3_result_list]

    local_result_list = download_results(associated_dataset_list, s3_result_list)

    return zip(associated_dataset_list, local_result_list)


def download_results(associated_dataset_list, s3_result_list):

    local_result_list = []
    output_dir = os.path.join(root_dir, 'results')

    for associated_dataset, s3_path in zip(associated_dataset_list, s3_result_list):
        output_file = os.path.join(output_dir, associated_dataset + '.csv')

        cmd = ['aws', 's3', 'cp', s3_path, output_file]
        subprocess.check_call(cmd, shell=True)

        local_result_list.append(output_file)

    return local_result_list
