import requests
import secrets
from retrying import retry
import log


def sync(api_dataset_id, s3_url, environment):
    token = secrets.get_api_token(environment)

    if environment == "prod":
        api_url = r"http://production-api.globalforestwatch.org"

    elif environment == "staging":
        api_url = r"http://staging-api.globalforestwatch.org"

    else:
        pass

    if environment in ["staging", "prod"]:
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {0}".format(token),
        }
        overwrite_dataset(headers, api_url, api_dataset_id, s3_url)


@retry(
    wait_exponential_multiplier=1000, wait_exponential_max=100000, stop_max_delay=100000
)
def make_request(
    headers, api_endpoint, request_type, payload=None, status_code_required=200
):
    if request_type == "GET":
        r = requests.get(api_endpoint, headers=headers)

    elif request_type == "POST":
        r = requests.post(api_endpoint, headers=headers, json=payload)

    elif request_type == "PATCH":
        r = requests.patch(api_endpoint, headers=headers, json=payload)

    else:
        raise ValueError("Unknown request_type {0}".format(request_type))

    if r.status_code == status_code_required:

        try:
            return_val = r.json()
        except ValueError:
            return_val = {"Response": "No JSON found"}

        return return_val

    else:
        log.error(r.text)
        raise ValueError("Request failed")


def overwrite_dataset(headers, api_url, dataset_id, s3_url):

    log.info("Overwriting dataset: {0}".format(dataset_id))
    dataset_url = r"{0}/dataset/{1}".format(api_url, dataset_id)

    status = _get_status(headers, dataset_url)
    name = _get_name(headers, dataset_url)

    if status != "saved":

        log.warning(
            'There was an issue while updating dataset "{}". Dataset with id {} not in saved status. Recover dataset.'.format(
                name, dataset_id
            ),
            True,
            dataset_id,
        )
        _recover(headers, dataset_url)

    _overwrite_dataset(headers, dataset_url, s3_url, name)


def _get_status(headers, dataset_url):
    log.info("Get dataset status: {0}".format(dataset_url))

    response = make_request(headers, dataset_url, "GET", status_code_required=200)
    return response["data"]["attributes"]["status"]


def _get_name(headers, dataset_url):
    log.info("Get dataset name: {0}".format(dataset_url))

    response = make_request(headers, dataset_url, "GET", status_code_required=200)
    return response["data"]["attributes"]["name"]


def _patch_overwrite(headers, dataset_url):
    modify_attributes_payload = {"overwrite": "true"}
    make_request(headers, dataset_url, "PATCH", modify_attributes_payload, 200)


def _recover(headers, dataset_url):
    make_request(
        headers, "{}/recover".format(dataset_url), "POST", status_code_required=200
    )


def _overwrite_dataset(headers, dataset_url, s3_url, name):
    _patch_overwrite(headers, dataset_url)

    data_overwrite_url = r"{0}/data-overwrite".format(dataset_url)
    overwrite_payload = {"url": s3_url, "provider": "csv"}

    log.debug("Headers : " + str(headers))
    log.debug("Data Overwrite : " + str(data_overwrite_url))
    log.debug("Overwrite Payload : " + str(overwrite_payload))

    make_request(headers, data_overwrite_url, "POST", overwrite_payload, 204)
    log.info('Successfully updated dataset "{}"'.format(name), True)
