import requests
from retrying import retry

import util


def sync(api_dataset_id, s3_url, environment):

    if environment == 'prod':
        api_url = r'http://production-api.globalforestwatch.org'
        token = util.load_json_from_token('dataset_api_creds.json')['token']

    elif environment == 'staging':
        api_url = r'http://staging-api.globalforestwatch.org'
        token = util.load_json_from_token('dataset_api_creds_staging.json')['token']

    else:
        pass

    if environment in ['staging', 'prod']:
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer {0}'.format(token)}
        overwrite_dataset(headers, api_url, api_dataset_id, s3_url)


@retry(wait_exponential_multiplier=1000, wait_exponential_max=100000, stop_max_delay=100000)
def make_request(headers, api_endpoint, request_type, payload, status_code_required):

    if request_type == 'POST':
        r = requests.post(api_endpoint, headers=headers, json=payload)

    elif request_type == 'PATCH':
        r = requests.patch(api_endpoint, headers=headers, json=payload)

    else:
        raise ValueError('Unknown request_type {0}'.format(request_type))

    if r.status_code == status_code_required:

        try:
            return_val = r.json()
        except ValueError:
            return_val = {"Response": "No JSON found"}

        return return_val

    else:
        print r.text
        raise ValueError("Request failed")


def overwrite_dataset(headers, api_url, dataset_id, s3_url):
    print 'Overwriting dataset: {0}'.format(dataset_id)
    dataset_url = r'{0}/dataset/{1}'.format(api_url, dataset_id)

    modify_attributes_payload = {"dataset": {"overwrite": True}}
    make_request(headers, dataset_url, 'PATCH', modify_attributes_payload, 200)

    data_overwrite_url = r'{0}/data-overwrite'.format(dataset_url)
    overwrite_payload = {"url": s3_url, "provider": "csv"}
    print 'Headers : ' +  str(headers)
    print 'Data Overwrite : ' + str(data_overwrite_url)
    print 'Overwrite Payload : ' + str(overwrite_payload)

    make_request(headers, data_overwrite_url, 'POST', overwrite_payload, 204)
