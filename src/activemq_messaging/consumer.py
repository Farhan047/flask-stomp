import stomp
from src.wiki_analytics import pageviews
from src.config import Config
from concurrent.futures import ThreadPoolExecutor
import uuid
import json
from dataclasses import dataclass
from enum import Enum
from typing import Dict
from typing import Callable
from stomp import Connection
from logging import Logger

logger: Logger = None


def set_logger(_logger):
    global logger
    logger = _logger


@dataclass(frozen=True)
class Payload:
    ack: Callable
    nack: Callable
    headers: Dict
    body: Dict


#
class Acknowledgements(Enum):
    """
    See more details at:
        - https://pubsub.github.io/pubsub-specification-1.2.html#SUBSCRIBE_ack_Header
        - https://jasonrbriggs.github.io/pubsub.py/api.html#acks-and-nacks
    """

    CLIENT = "client"
    CLIENT_INDIVIDUAL = "client-individual"
    AUTO = "auto"


class PageViewsListener(stomp.ConnectionListener):
    def __init__(
            self,
            connection: Connection,
            subscription_id: str,
            config: Config,
            should_process_msg_on_background: bool
    ) -> None:
        self._connection = connection
        self._subscription_id = subscription_id
        self._config = config
        self._should_process_msg_on_background = should_process_msg_on_background

        pageviews.set_logger(logger)
        pageviews.set_config(config)
        self._pool_executor = self._create_new_worker_executor()

    def _create_new_worker_executor(self):
        return ThreadPoolExecutor(max_workers=self._config.max_listener_workers, thread_name_prefix="listener-thread")

    def on_message(self, headers, body):
        message_id = headers["message-id"]

        # logger.info("Message ID : {}".format(message_id))
        # logger.info("Subscription ID : {}".format(self._subscription_id))

        def ack_logic():
            self._connection.ack(message_id, self._subscription_id)

        def nack_logic():
            self._connection.nack(message_id, self._subscription_id, requeue=False)

        payload = Payload(ack_logic, nack_logic, headers, json.loads(body))

        try:

            if self._should_process_msg_on_background:
                self._pool_executor.submit(pageviews.run_execution_on_message, payload)
            else:
                pageviews.run_execution_on_message(payload)

        except Exception as e:
            logger.error("Error while submitting on the listener thread pool")
            logger.error(e)

        # read_messages.append({'id': headers['message-id'], 'subscription': headers['subscription']})

    def shutdown_threadpool_executor(self):
        self._pool_executor.shutdown(wait=False)


def create_connection(config: Config):

    connection = stomp.Connection([(config.active_mq_host, 61613)])
    subscription_id = "listener-" + str(uuid.uuid4())
    # logger.info("Building lister with subscription_id: {}".format(subscription_id))
    listener = PageViewsListener(
        connection=connection,
        subscription_id=subscription_id,
        config=config,
        should_process_msg_on_background=True
    )
    connection.set_listener('algo-execution', listener)
    connection.connect(wait=True)
    ack_mode = Acknowledgements.CLIENT.value
    connection.subscribe(
        config.active_mq_queue,
        id=subscription_id,
        ack=ack_mode,
        headers={'activemq.prefetchSize': config.activemq_prefetchSize}
    )
    logger.info("Successfully Connected and subscribed, ack_mode : {}".format(ack_mode))
    return connection, listener
