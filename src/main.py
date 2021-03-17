import multiprocessing as mp
from flask import Flask
import random
import logging
from logging.handlers import QueueHandler, QueueListener

import stomp
import time
from src.wiki_analytics import pageviews
from concurrent.futures import ThreadPoolExecutor
from src.activemq_messaging import consumer
from src.config import Config


class ActiveMQConnectionError(Exception):
    pass


class ActiveMQConnection(mp.Process):
    def __init__(
            self,
            name,
            logging_queue,
            config):
        mp.Process.__init__(self)
        self.name = name
        self.logging_queue = logging_queue
        self.config: Config = config

    def run(self) -> None:
        logger = worker_init(self.logging_queue, logger_name=self.name, config=self.config)
        consumer.set_logger(logger)

        try:
            connection, listener = consumer.create_connection(config=self.config)
        except stomp.exception.ConnectFailedException:
            raise ActiveMQConnectionError("Connection failed, check if ActiveMQ server is up and running")


        while True:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                logger.error("Cancelled By User")
                connection.disconnect()
                listener.shutdown_threadpool_executor()
                break


class FlaskProcess(mp.Process):
    def __init__(self, name, logging_queue, config):
        mp.Process.__init__(self)
        self.name = name
        self.logging_queue = logging_queue
        self.config = config

    def run(self) -> None:
        logger = worker_init(self.logging_queue, logger_name=self.name, config=self.config)
        app = Flask(__name__)
        data_store = []
        app.logger = logger

        @app.route('/')
        def hello_world():
            data_store.append(str(random.randint(0, 100)))
            app.logger.info("API Hit!!")
            return '\n'.join(data_store)

        app.run(host='0.0.0.0', port=5000, use_reloader=False)


def logger_init(config):
    q = mp.Queue()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "[%(asctime)s] [%(processName)s] [%(threadName)s] %(levelname)s in %(filename)s @ %(lineno)d: %(message)s"
    ))
    ql = QueueListener(q, handler)
    ql.start()

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return ql, q, logger


def worker_init(q, logger_name, config):
    qh = QueueHandler(q)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(qh)
    return logger


def main():
    mp.set_start_method('spawn')
    config = Config
    q_listener, q, logger = logger_init(config)
    logger.info("config : {}".format(config))

    all_processes = []

    # Start Flask Process
    flask_process = FlaskProcess(name="flask-app", logging_queue=q, config=config)
    flask_process.start()
    all_processes.append(flask_process)
    logger.info("Flask Process Started")

    # Process for ActiveMQ connection on STOMP.
    logger.info("Starting {} ActiveMQConnection process with {} listener threads each".format(config.max_connection_prx,
                                                                                              config.max_listener_workers))
    for each in range(config.max_connection_prx):
        listener_process = ActiveMQConnection(
            name="active-mq-connection-{}".format(each),
            logging_queue=q,
            config=config
        )
        listener_process.start()
        all_processes.append(listener_process)

    for each in all_processes:
        each.join()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.error("Cancelled by User")
