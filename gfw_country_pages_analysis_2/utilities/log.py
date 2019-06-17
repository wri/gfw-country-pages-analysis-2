import secrets
import requests
import logging

logger = logging.getLogger("gfw-sync")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(formatter)

fh = logging.FileHandler("gfw-sync.log")
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

logger.addHandler(sh)
logger.addHandler(fh)


def debug(message, slack=False, dataset_id=None):
    logger.debug(message)


def info(message, slack=False, dataset_id=None):
    logger.info(message)
    if slack:
        slack_webhook("INFO", message, dataset_id)


def warning(message, slack=False, dataset_id=None):
    logger.warning(message)
    if slack:
        slack_webhook("WARNING", message, dataset_id)


def error(message, slack=False, dataset_id=None):
    logger.error(message)
    if slack:
        slack_webhook("ERROR", message, dataset_id)


def critical(message, slack=False, dataset_id=None):
    logger.critical(message)
    if slack:
        slack_webhook("CRITICAL", message, dataset_id)


def slack_webhook(level, message, dataset_id=None):

    app = "GFW SYNC country page analysis 2"

    # actions = None
    if level.upper() == "WARNING":
        color = "#E2AC37"
    elif level.upper() == "ERROR" or level.upper() == "CRITICAL":
        color = "#FF0000"
        # actions = [
        #     {
        #         "name": "I am on it",
        #         "text": "I am on it",
        #         "type": "button",
        #         "value": "I am on it",
        #         "style": "primary",
        #     }
        # ]
        # if dataset_id:
        #     actions.append(
        #         {
        #             "type": "button",
        #             "text": "Check dataset status",
        #             "url": "https://production-api.globalforestwatch.org/v1/dataset/{}".format(
        #                 dataset_id
        #             ),
        #         }
        #     )

    else:
        color = "#36A64F"

    attachement = {
        "attachments": [
            {
                "fallback": "{} - {} - {}".format(app, level.upper(), message),
                # "callback_id": "dataset_not_saved",
                "color": color,
                "title": app,
                "fields": [{"title": level.upper(), "value": message, "short": False}],
            }
        ]
    }
    # if actions:
    #     attachement["attachments"][0]["actions"] = actions

    url = secrets.get_slack_webhook("data-updates")
    return requests.post(url, json=attachement)
