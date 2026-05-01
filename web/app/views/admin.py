from flask import Blueprint, render_template

bp = Blueprint("admin", __name__)


@bp.get("/")
def index():
    return render_template("admin/index.html")
