from flask import Blueprint, render_template, request
from flask_login import login_required

from app.db import get_session

bp = Blueprint("admin", __name__)

PAGE_SIZE = 50


def _page() -> int:
    try:
        p = int(request.args.get("page", 1))
        return max(1, p)
    except ValueError:
        return 1


def _paginate(rows, page: int, size: int = PAGE_SIZE):
    """Simple in-memory pagination on a Cassandra result set."""
    all_rows = list(rows)
    total = len(all_rows)
    start = (page - 1) * size
    return all_rows[start : start + size], total


# ── Overview ──────────────────────────────────────────────────────────────────
@bp.get("/")
@login_required
def index():
    session = get_session()
    counts = {}
    for table in ("users", "items", "proxies", "sessions"):
        try:
            row = session.execute(f"SELECT COUNT(*) FROM {table}").one()  # noqa: S608
            counts[table] = row[0]
        except Exception:
            counts[table] = "N/A"
    return render_template("admin/index.html", counts=counts)


# ── Users ─────────────────────────────────────────────────────────────────────
@bp.get("/users")
@login_required
def users():
    session = get_session()
    page = _page()
    rows = session.execute(
        "SELECT id, email, username, is_active, created_at FROM users"
    )
    data, total = _paginate(rows, page)
    return render_template("admin/users.html", rows=data, page=page, total=total, page_size=PAGE_SIZE)


# ── Items ─────────────────────────────────────────────────────────────────────
@bp.get("/items")
@login_required
def items():
    session = get_session()
    page = _page()
    rows = session.execute(
        "SELECT id, name, description, owner_id, created_at FROM items"
    )
    data, total = _paginate(rows, page)
    return render_template("admin/items.html", rows=data, page=page, total=total, page_size=PAGE_SIZE)


# ── Proxies ───────────────────────────────────────────────────────────────────
@bp.get("/proxies")
@login_required
def proxies():
    session = get_session()
    page = _page()
    protocol = request.args.get("protocol", "")
    if protocol:
        rows = session.execute(
            "SELECT protocol, ip, port, source, is_active, first_seen, last_seen FROM proxies WHERE protocol = %s",
            (protocol,),
        )
    else:
        rows = session.execute(
            "SELECT protocol, ip, port, source, is_active, first_seen, last_seen FROM proxies"
        )
    data, total = _paginate(rows, page)
    return render_template(
        "admin/proxies.html",
        rows=data,
        page=page,
        total=total,
        page_size=PAGE_SIZE,
        protocol=protocol,
        protocols=["", "http", "socks4", "socks5"],
    )


# ── Sessions ──────────────────────────────────────────────────────────────────
@bp.get("/sessions")
@login_required
def sessions():
    session = get_session()
    page = _page()
    rows = session.execute('SELECT "token", user_id, expires_at FROM sessions')
    data, total = _paginate(rows, page)
    return render_template("admin/sessions.html", rows=data, page=page, total=total, page_size=PAGE_SIZE)
