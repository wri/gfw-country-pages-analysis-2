import boto3
import json

client = boto3.client("secretsmanager")


def get_google_credentials(account):

    response = client.get_secret_value(
        SecretId="google_cloud/{}".format(account)
    )
    return json.loads(response["SecretString"])


def get_api_token(env):

    print env
    response = client.get_secret_value(
        SecretId="gfw-api/{}-token".format(env)
    )
    return json.loads(response["SecretString"])["token"]


def get_slack_webhook(channel):
    response = client.get_secret_value(
        SecretId="slack/gfw-sync"
    )
    return json.loads(response["SecretString"])[channel]