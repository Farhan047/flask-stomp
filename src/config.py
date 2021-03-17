from dataclasses import dataclass
import os


@dataclass
class Config:
    active_mq_host: str = os.getenv("ACTIVE_MQ_HOST", "")
    active_mq_username: str = os.getenv("ACTIVE_MQ_USERNAME", "")
    active_mq_password: str = os.getenv("ACTIVE_MQ_PASSWORD", "")
    active_mq_queue = "/queue/some_queue"
    max_connection_prx = 3
    max_listener_workers = 2
    activemq_prefetchSize = 3
