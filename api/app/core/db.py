import time
import logging

from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy

from app.core.config import settings

logger = logging.getLogger(__name__)
_session = None


def init_db(retries: int = 10, delay: int = 10):
    global _session
    auth = PlainTextAuthProvider(
        username=settings.CASSANDRA_USER,
        password=settings.CASSANDRA_PASSWORD,
    )
    cluster = Cluster(
        contact_points=settings.CASSANDRA_HOSTS,
        auth_provider=auth,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="dc1"),
        protocol_version=4,
    )
    for attempt in range(1, retries + 1):
        try:
            _session = cluster.connect(settings.CASSANDRA_KEYSPACE)
            logger.info("Cassandra connected on attempt %d", attempt)
            return _session
        except NoHostAvailable as exc:
            if attempt == retries:
                raise
            logger.warning(
                "Cassandra not ready (attempt %d/%d): %s – retrying in %ds",
                attempt, retries, exc, delay,
            )
            time.sleep(delay)


def get_session():
    if _session is None:
        init_db()
    return _session
