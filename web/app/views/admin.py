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
    for table in ("users", "items", "proxies", "golden_proxies", "sessions"):
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


# ── Fresh Proxies ────────────────────────────────────────────────────────────
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


# ── Containers ───────────────────────────────────────────────────────────────
@bp.get("/containers")
@login_required
def containers():
    import socket as _socket
    try:
        import docker as docker_sdk
        client = docker_sdk.from_env()
        raw = client.containers.list(all=False)  # running only
        rows = []
        for c in raw:
            ports = []
            for container_port, bindings in (c.ports or {}).items():
                if bindings:
                    for b in bindings:
                        ports.append(f"{b['HostPort']}→{container_port}")
                else:
                    ports.append(container_port)
            rows.append({
                "id": c.short_id,
                "name": c.name,
                "image": c.image.tags[0] if c.image.tags else c.image.short_id,
                "status": c.status,
                "created": c.attrs.get("Created", "")[:19].replace("T", " "),
                "ports": ", ".join(ports) or "—",
            })
        error = None
    except Exception as exc:
        rows = []
        error = str(exc)
    hostname = _socket.gethostname()
    return render_template("admin/containers.html", rows=rows, error=error, hostname=hostname)


# ── Golden Proxies ───────────────────────────────────────────────────────────
@bp.get("/golden-proxies")
@login_required
def golden_proxies():
    session = get_session()
    page = _page()
    protocol = request.args.get("protocol", "")
    if protocol:
        rows = session.execute(
            "SELECT protocol, ip, port, source, first_seen, last_seen FROM golden_proxies WHERE protocol = %s",
            (protocol,),
        )
    else:
        rows = session.execute(
            "SELECT protocol, ip, port, source, first_seen, last_seen FROM golden_proxies"
        )
    data, total = _paginate(rows, page)
    return render_template(
        "admin/golden_proxies.html",
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
