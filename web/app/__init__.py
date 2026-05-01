from flask import Flask
from prometheus_flask_exporter import PrometheusMetrics


def create_app():
    app = Flask(__name__)
    app.config.from_envvar("FLASK_SETTINGS", silent=True)

    # Prometheus metrics at /metrics
    PrometheusMetrics(app)

    from app.views.dashboard import bp as dashboard_bp
    from app.views.admin import bp as admin_bp

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(admin_bp, url_prefix="/admin")

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app
