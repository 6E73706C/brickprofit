import os

from flask import Flask
from flask_wtf.csrf import CSRFProtect
from prometheus_flask_exporter import PrometheusMetrics

from app.auth import login_manager

csrf = CSRFProtect()


def create_app():
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "change-me")
    app.config["WTF_CSRF_TIME_LIMIT"] = 3600
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = True
    app.config.from_envvar("FLASK_SETTINGS", silent=True)

    csrf.init_app(app)
    login_manager.init_app(app)

    # Prometheus metrics at /metrics
    PrometheusMetrics(app)

    # Background Cassandra → Prometheus proxy-count collector
    from app.proxy_metrics import start_collector
    start_collector()

    # Background Cassandra → Prometheus LEGO-set count collector
    from app.lego_metrics import start_collector as start_lego_collector
    start_lego_collector()

    from app.views.auth import bp as auth_bp
    from app.views.dashboard import bp as dashboard_bp
    from app.views.admin import bp as admin_bp

    if "auth" not in app.blueprints:
        app.register_blueprint(auth_bp)
    if "dashboard" not in app.blueprints:
        app.register_blueprint(dashboard_bp)
    if "admin" not in app.blueprints:
        app.register_blueprint(admin_bp, url_prefix="/admin")

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app
