from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy

from app.core.config import settings

_session = None


def init_db():
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
    _session = cluster.connect(settings.CASSANDRA_KEYSPACE)
    return _session


def get_session():
    if _session is None:
        init_db()
    return _session
