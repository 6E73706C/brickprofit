from flask import Blueprint, render_template

bp = Blueprint("dashboard", __name__)


@bp.get("/")
def index():
    return render_template("dashboard/index.html")
