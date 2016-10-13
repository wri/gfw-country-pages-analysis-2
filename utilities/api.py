import requests
from retrying import retry

import util

api_url = r'http://production-api.globalforestwatch.org'
# api_url = r'http://api.resourcewatch.org'
# api_url = r'http://staging-api.globalforestwatch.org'

token = util.load_json_from_token('dataset_api_creds.json')['token']
headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer {0}'.format(token)}


def sync(api_dataset_id, s3_url):

    truncate_dataset(api_dataset_id)

    append_from_json_file(api_dataset_id, s3_url)

    confirm_dataset_saved(api_dataset_id)


def make_request(api_endpoint, request_type, payload, status_code_required, json_map_list=None):
    # print 'Sending {0} request to endpoint {1}'.format(request_type, api_endpoint)

    if request_type == 'GET':
        r = requests.get(api_endpoint, headers=headers)

    elif request_type == 'POST':
        r = requests.post(api_endpoint, headers=headers, json=payload)

    elif request_type == 'PUT':
        r = requests.put(api_endpoint, headers=headers, json=payload)

    elif request_type == 'DELETE':
        r = requests.delete(api_endpoint, headers=headers, json=payload)

    else:
        raise ValueError('Unknown request_type {0}'.format(request_type))

    if r.status_code == status_code_required:

        if json_map_list:
            # http://stackoverflow.com/questions/14692690
            return_val = reduce(lambda d, k: d[k], json_map_list, r.json())

        else:
            return_val = r.json()

        return return_val

    else:
        print r.text
        raise ValueError("Request failed")


def create_dataset(s3_path):
    datasets_url = r'{0}/datasets'.format(api_url)
    payload = {"dataset": {"connector_type": "json", "connector_provider": "rwjson",
                           "dataset_attributes":
                               {"application": ["gfw"], "name": "Terra-I alerts tabulated by GADM1 boundaries",
                                "data_path": "data",
                                "tags": ["terrai", "terra-i", "gadm1"]},
                           "connector_url": s3_path
                           }}

    dataset_id = make_request(datasets_url, 'POST', payload, 201, ['id'])

    print 'Created new dataset:\n{0}'.format(dataset_id)

    return dataset_id


def append_from_json_file(dataset_id, s3_url):

    print 'Appending from JSON file {0} to {1}'.format(s3_url, dataset_id)

    dataset_url = r'{0}/datasets/{1}'.format(api_url, dataset_id)
    new_data_url = r'{0}/data'.format(dataset_url)

    payload = {"dataset": {"connector_url": s3_url, "data_path": "data"}}

    make_request(new_data_url, 'POST', payload, 200)


def check_creation_status(dataset_id):
    dataset_url = r'{0}/datasets/{1}'.format(api_url, dataset_id)

    dataset_status = make_request(dataset_url, 'GET', None, 200, ['meta', 'status'])

    return dataset_status


def truncate_dataset(dataset_id):

    print 'Truncating dataset: {0}'.format(dataset_id)
    dataset_url = r'{0}/datasets/{1}'.format(api_url, dataset_id)

    modify_attributes_payload = {"dataset": {"dataset_attributes": {"data_overwrite": True}}}
    make_request(dataset_url, 'PUT', modify_attributes_payload, 200)

    data_overwrite_url = r'{0}/data-overwrite'.format(dataset_url)
    overwrite_payload = {"dataset": {"data": []}}

    make_request(data_overwrite_url, 'POST', overwrite_payload, 200)


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=10000)
def confirm_dataset_saved(dataset_id):

    dataset_status = check_creation_status(dataset_id)

    if dataset_status != 'saved':
        raise ValueError('Dataset {0} status is {1}'.format(dataset_id, dataset_status))


def delete_dataset(dataset_id):

    dataset_url = r'{0}/datasets/{1}'.format(api_url, dataset_id)

    payload = {"dataset": {"dataset_attributes": {"data_overwrite": True}}}

    dataset_status = make_request(dataset_url, 'DELETE', payload, 200)

    print dataset_status