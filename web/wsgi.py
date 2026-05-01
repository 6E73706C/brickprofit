import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
log.info("wsgi.py loading – calling create_app() once")

from app import create_app

app = create_app()
log.info("create_app() complete – app ready")
