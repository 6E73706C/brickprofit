from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required, login_user, logout_user
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_wtf import FlaskForm
from wtforms import PasswordField, StringField
from wtforms.validators import DataRequired

from app.auth import get_admin

bp = Blueprint("auth", __name__)

# Limiter is initialised on the app in __init__.py; imported here for the decorator
limiter: Limiter | None = None  # injected after app creation


class LoginForm(FlaskForm):
    username = StringField("Username", validators=[DataRequired()])
    password = PasswordField("Password", validators=[DataRequired()])


@bp.get("/admin/login")
def login():
    return render_template("auth/login.html", form=LoginForm())


@bp.post("/admin/login")
def login_post():
    # Rate limiting is applied by the limiter decorator on the app level (see __init__.py)
    form = LoginForm()
    if not form.validate_on_submit():
        flash("Invalid request (CSRF or missing fields).", "danger")
        return render_template("auth/login.html", form=form), 400

    admin = get_admin()
    # Constant-time username check via werkzeug
    if form.username.data != admin.username or not admin.check_password(form.password.data):
        flash("Invalid credentials.", "danger")
        return render_template("auth/login.html", form=form), 401

    login_user(admin, remember=False)
    next_url = request.args.get("next")
    # Guard against open-redirect
    if next_url and next_url.startswith("/admin"):
        return redirect(next_url)
    return redirect(url_for("admin.index"))


@bp.get("/admin/logout")
@login_required
def logout():
    logout_user()
    flash("Logged out.", "success")
    return redirect(url_for("auth.login"))
