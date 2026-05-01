import json
import logging
import os
import time

log = logging.getLogger(__name__)

_session = None


def _parse_hosts(raw: str) -> list[str]:
    raw = raw.strip()
    if raw.startswith("["):
        return json.loads(raw)
    return [h.strip() for h in raw.split(",")]


def get_session():
    global _session
    if _session is None:
        # Lazy import so cassandra-driver doesn't block app startup
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import Cluster, NoHostAvailable
        from cassandra.policies import DCAwareRoundRobinPolicy

        hosts = _parse_hosts(os.environ.get("CASSANDRA_HOSTS", "cassandra1,cassandra2,cassandra3"))
        keyspace = os.environ.get("CASSANDRA_KEYSPACE", "brickprofit")
        user = os.environ.get("CASSANDRA_USER", "cassandra")
        password = os.environ.get("CASSANDRA_PASSWORD", "cassandra")

        auth = PlainTextAuthProvider(user, password)
        for attempt in range(1, 16):
            try:
                cluster = Cluster(
                    hosts,
                    auth_provider=auth,
                    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
                    protocol_version=4,
                )
                _session = cluster.connect(keyspace)
                log.info("Cassandra connected.")
                break
            except NoHostAvailable as exc:
                log.warning("Cassandra not ready (attempt %d/15): %s", attempt, exc)
                if attempt == 15:
                    raise
                time.sleep(10)
    return _session
