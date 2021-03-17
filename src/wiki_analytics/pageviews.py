from logging import Logger
import time
from src.config import Config

logger: Logger = None
config: Config = None
from src.activemq_messaging.consumer import Payload


def set_logger(_logger):
    global logger
    logger = _logger


def set_config(_config):
    global config
    config = _config


def run_execution_on_message(payload: Payload):
    seconds = 10
    try:
        time.sleep(seconds)
        if payload.body['msg_id'] % 2 == 0:
            raise Exception("Divisible by 2")

        logger.info("Acknowledged : {}".format(payload.body))
        payload.ack()
    except:
        payload.nack()
        logger.info("Negative Acknowledged : {}".format(payload.body))
