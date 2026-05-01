import os

from flask import Flask
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_wtf.csrf import CSRFProtect
from prometheus_flask_exporter import PrometheusMetrics

from app.auth import login_manager

csrf = CSRFProtect()
limiter = Limiter(get_remote_address, default_limits=[])


def create_app():
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "change-me")
    app.config["WTF_CSRF_TIME_LIMIT"] = 3600
    # Secure session cookie settings
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = True
    app.config.from_envvar("FLASK_SETTINGS", silent=True)

    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/1")
    app.config["RATELIMIT_STORAGE_URI"] = redis_url
    limiter.init_app(app)
    csrf.init_app(app)
    login_manager.init_app(app)

    # Prometheus metrics at /metrics
    PrometheusMetrics(app)

    from app.views.auth import bp as auth_bp
    from app.views.dashboard import bp as dashboard_bp
    from app.views.admin import bp as admin_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(admin_bp, url_prefix="/admin")

    # Rate-limit login POST: 10 attempts / minute / IP, brute-force protection
    limiter.limit("10 per minute")(app.view_functions["auth.login_post"])

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app
