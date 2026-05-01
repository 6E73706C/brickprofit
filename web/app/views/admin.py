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
    try:
        import docker as docker_sdk
        client = docker_sdk.from_env()

        # Build node_id → display label map
        node_map = {}
        try:
            for n in client.nodes.list():
                desc = n.attrs.get("Description", {})
                hostname = desc.get("Hostname", n.short_id)
                addr = n.attrs.get("Status", {}).get("Addr", "")
                # Status.Addr is 0.0.0.0 for the local manager node;
                # fall back to ManagerStatus.Addr which carries the real IP.
                if not addr or addr == "0.0.0.0":
                    mgr_addr = n.attrs.get("ManagerStatus", {}).get("Addr", "")
                    addr = mgr_addr.split(":")[0] if mgr_addr else ""
                node_map[n.id] = f"{hostname} ({addr})" if addr else hostname
        except Exception:
            pass  # not a swarm manager – fall through to local-only

        # Build service_id → service_name map
        svc_map = {}
        try:
            for s in client.services.list():
                svc_map[s.id] = s.name
        except Exception:
            pass

        rows = []

        if node_map:
            # Swarm mode: query all tasks across all nodes
            try:
                tasks = client.api.tasks()
            except Exception:
                tasks = []
            for t in tasks:
                state = (t.get("Status") or {}).get("State", "")
                if state not in ("running", "starting", "preparing"):
                    continue
                try:
                    spec = t.get("Spec", {})
                    container_spec = spec.get("ContainerSpec", {})
                    image = container_spec.get("Image", "")
                    # strip digest suffix  (image@sha256:...)
                    if "@" in image:
                        image = image.split("@")[0]
                    svc_id = t.get("ServiceID", "")
                    svc_name = svc_map.get(svc_id, svc_id[:12] if svc_id else "—")
                    slot = t.get("Slot")
                    name = f"{svc_name}.{slot}" if slot else svc_name
                    node_id = t.get("NodeID", "")
                    node_label = node_map.get(node_id, node_id[:12] if node_id else "—")
                    container_status = (t.get("Status") or {}).get("ContainerStatus") or {}
                    cid = container_status.get("ContainerID", "")
                    created = t.get("CreatedAt", "")[:19].replace("T", " ")
                    rows.append({
                        "id": cid[:12] if cid else "—",
                        "name": name,
                        "image": image,
                        "status": state,
                        "node": node_label,
                        "created": created,
                        "ports": "—",
                    })
                except Exception:
                    pass
            # Sort by node label then container name
            rows.sort(key=lambda r: (r["node"], r["name"]))
        else:
            # Fallback: local containers only (non-swarm or no manager access)
            raw = client.containers.list(all=False)
            for c in raw:
                try:
                    ports = []
                    for container_port, bindings in (c.ports or {}).items():
                        if bindings:
                            for b in bindings:
                                ports.append(f"{b['HostPort']}→{container_port}")
                        else:
                            ports.append(container_port)
                    image_name = (c.attrs.get("Config") or {}).get("Image") or c.short_id
                    rows.append({
                        "id": c.short_id,
                        "name": c.name,
                        "image": image_name,
                        "status": c.status,
                        "node": "local",
                        "created": c.attrs.get("Created", "")[:19].replace("T", " "),
                        "ports": ", ".join(ports) or "—",
                    })
                except Exception:
                    pass

        error = None
    except Exception as exc:
        rows = []
        node_map = {}
        error = str(exc)

    swarm_mode = bool(node_map)
    return render_template(
        "admin/containers.html",
        rows=rows,
        error=error,
        swarm_mode=swarm_mode,
        node_count=len(node_map),
    )


# ── Golden Proxies ───────────────────────────────────────────────────────────
@bp.get("/golden-proxies")
@login_required
def golden_proxies():
    session = get_session()
    page = _page()
    protocol = request.args.get("protocol", "")
    try:
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
        error = None
    except Exception as exc:
        data, total, error = [], 0, str(exc)
    return render_template(
        "admin/golden_proxies.html",
        rows=data,
        page=page,
        total=total,
        page_size=PAGE_SIZE,
        protocol=protocol,
        protocols=["", "http", "socks4", "socks5"],
        error=error,
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
