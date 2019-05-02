import secrets
import requests
import logging


logger = logging.getLogger('gfw-sync')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(formatter)

fh = logging.FileHandler("gfw-sync.log")
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

logger.addHandler(sh)
logger.addHandler(fh)


def debug(message):
    logger.debug(message)


def info(message, slack=False):
    logger.info(message)
    if slack:
        slack_webhook("INFO", message)


def warning(message, slack=False):
    logger.warning(message)
    if slack:
        slack_webhook("WARNING", message)


def error(message, slack=False):
    logger.error(message)
    if slack:
        slack_webhook("ERROR", message)


def critical(message, slack=False):
    logger.critical(message)
    if slack:
        slack_webhook("CRITICAL", message)


def slack_webhook(level, message):

    app = "GFW SYNC country page analysis 2"

    if level.upper() == "WARNING":
        color = "#E2AC37"
    elif level.upper() == "ERROR" or level.upper() == "CRITICAL":
        color = "#FF0000"
    else:
        color = "#36A64F"

    attachement = {
        "attachments": [
            {
                "fallback": "{} - {} - {}".format(app, level.upper(), message),
                "color": color,
                "title": app,
                "fields": [{"title": level.upper(), "value": message, "short": False}],
            }
        ]
    }
    url = secrets.get_slack_webhook("data-updates")
    requests.get(url, json=attachement)
