import os
import time

import redis as redis_client
from flask import Blueprint, abort, flash, redirect, render_template, request, url_for
from flask_login import login_required, login_user, logout_user
from flask_wtf import FlaskForm
from wtforms import PasswordField, StringField
from wtforms.validators import DataRequired

from app.auth import get_admin

bp = Blueprint("auth", __name__)

_redis = None


def _get_redis():
    global _redis
    if _redis is None:
        try:
            url = os.environ.get("REDIS_URL", "redis://redis:6379/1")
            _redis = redis_client.from_url(url, decode_responses=True, socket_connect_timeout=2)
        except Exception:
            pass
    return _redis


def _rate_limit(key: str, limit: int = 10, window: int = 60) -> bool:
    """Returns True (allow) or False (block). Uses Redis sliding counter."""
    r = _get_redis()
    if r is None:
        return True  # Redis down → fail open (don't block legitimate users)
    try:
        pipe = r.pipeline()
        pipe.incr(key)
        pipe.expire(key, window)
        result = pipe.execute()
        return result[0] <= limit
    except Exception:
        return True  # Redis error → fail open


class LoginForm(FlaskForm):
    username = StringField("Username", validators=[DataRequired()])
    password = PasswordField("Password", validators=[DataRequired()])


@bp.get("/admin/login")
def login():
    return render_template("auth/login.html", form=LoginForm())


@bp.post("/admin/login")
def login_post():
    ip = request.headers.get("X-Forwarded-For", request.remote_addr).split(",")[0].strip()
    if not _rate_limit(f"login:{ip}", limit=10, window=60):
        abort(429)

    form = LoginForm()
    if not form.validate_on_submit():
        flash("Invalid request (CSRF or missing fields).", "danger")
        return render_template("auth/login.html", form=form), 400

    admin = get_admin()
    if form.username.data != admin.username or not admin.check_password(form.password.data):
        flash("Invalid credentials.", "danger")
        return render_template("auth/login.html", form=form), 401

    login_user(admin, remember=False)
    next_url = request.args.get("next")
    if next_url and next_url.startswith("/admin"):
        return redirect(next_url)
    return redirect(url_for("admin.index"))


@bp.get("/admin/logout")
@login_required
def logout():
    logout_user()
    flash("Logged out.", "success")
    return redirect(url_for("auth.login"))
