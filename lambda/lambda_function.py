import boto3


CLIENT = boto3.client("batch")
S3 = boto3.resource("s3")
STATUS = S3.Object("gfw2-data", "forest_change/umd_landsat_alerts/prod/events/status")


def lambda_handler(event, context):

    status = get_status()

    if status == "SAVED":

        response = CLIENT.submit_job(
            jobDefinition="country-page-updates-job-definition",
            jobName="glad-country-page-updates",
            jobQueue="glad-country-pages-update-queue",
            containerOverrides={
                "command": [
                    "update_country_stats",
                    "-d",
                    "umd_landsat_alerts",
                    "-e",
                    "prod",
                ]
            },
        )

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": response,
        }

    else:
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": {"Status": status, "Action": "No action taken"},
        }


def get_status():
    body = STATUS.get()["Body"].read().strip().decode("utf-8")
    return body
