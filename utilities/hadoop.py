import os
import sys
import subprocess
import uuid

import google_sheet as gs

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
external_dir = os.path.dirname(root_dir)

sys.path.append(external_dir)

# The hadoop_pip package must be in the external dir referenced above, i.e.:
# Make sure that the folder is hadoop_pip, not hadoop-pip on your local file system
# - Desktop
#    - gfw-country-pages-analysis-2
#    - hadoop_pip

from hadoop_pip import run_pip


def pip(dataset_technical_name, environment):

    config = gs.get_hadoop_config(dataset_technical_name, environment)

    # Run hadoop process only if the environment is prod/staging
    if environment in ['prod', 'staging']:

        if dataset_technical_name == 'umd_landsat_alerts':
            instance_count = 12
        else:
            instance_count = 6

        s3_result_list = run_pip.run([config], instance_count=instance_count)

        # Response from hadoop comes back as list of lists
        # Example: [[s3://gfw2-data/alerts-tsv/hadoop-jobs/bb858284-8c4d-4e00-8473-69cef650a7f3/output1.csv"]]

        # Grab s3 output
        s3_result = [x[0] for x in s3_result_list][0]
        local_file = download_result(s3_result)

    else:
        # example terrai results - used for testing
        local_file = r'D:\scripts\gfw-country-pages-analysis-2\results\270f8e87-f0e8-4ade-a585-7213ed8b4c27\output.csv'

    return local_file


def download_result(s3_path):

    # generate unique id
    guid = str(uuid.uuid4())
    output_dir = os.path.join(root_dir, 'results', guid)
    os.mkdir(output_dir)

    output_file = os.path.join(output_dir, 'output.csv')
    print 'Downloading {} to {}'.format(s3_path, output_file)

    shell = False
    if os.name == 'nt':
        shell = True

    cmd = ['aws', 's3', 'cp', s3_path, output_file]
    subprocess.check_call(cmd, shell=shell)

    return output_file
