import os
import re
import threading
import time
from pathlib import Path

import requests as _requests
from flask import Blueprint, jsonify, render_template, request, send_from_directory
from flask_login import login_required

from app.db import get_session

bp = Blueprint("admin", __name__)

LEGO_IMAGES_DIR = os.environ.get("LEGO_IMAGES_DIR", "/data/lego-images")

PAGE_SIZE = 50

# ── Image backfill ────────────────────────────────────────────────────────────
_backfill_lock = threading.Lock()
_backfill_state: dict = {"running": False, "done": 0, "total": 0, "errors": 0, "started_at": None}


def _safe_filename(item_no: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", item_no) + ".png"


def _backfill_worker(rows: list) -> None:
    global _backfill_state
    images_dir = Path(LEGO_IMAGES_DIR)
    images_dir.mkdir(parents=True, exist_ok=True)
    done = 0
    errors = 0
    for row in rows:
        try:
            item_no = row.item_no
            image_url = row.image_url
            if not item_no or not image_url:
                done += 1
                continue
            # convert thumbnail URL to large image URL (same logic as scraper)
            large_url = image_url.replace("/ItemImage/ST/", "/ItemImage/SL/")
            large_url = re.sub(r"\.t1\.png$", ".png", large_url)
            dest = images_dir / _safe_filename(item_no)
            if dest.exists():
                done += 1
                with _backfill_lock:
                    _backfill_state["done"] = done
                continue
            tmp = dest.with_suffix(".tmp")
            try:
                resp = _requests.get(large_url, timeout=15, stream=True)
                resp.raise_for_status()
                with open(tmp, "wb") as fh:
                    for chunk in resp.iter_content(65536):
                        fh.write(chunk)
                tmp.rename(dest)
            except Exception:
                errors += 1
                if tmp.exists():
                    tmp.unlink(missing_ok=True)
            done += 1
            with _backfill_lock:
                _backfill_state["done"] = done
                _backfill_state["errors"] = errors
            time.sleep(0.1)  # be polite, ~10 req/s
        except Exception:
            errors += 1
            done += 1
    with _backfill_lock:
        _backfill_state["running"] = False
        _backfill_state["done"] = done
        _backfill_state["errors"] = errors


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
    for table in ("users", "items", "proxies", "golden_proxies", "sessions", "lego_sets"):
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


# ── LEGO Set images ────────────────────────────────────────────────────────────
@bp.get("/media/lego/<path:filename>")
@login_required
def lego_image(filename):
    """Serve locally-cached LEGO set images from the shared volume."""
    return send_from_directory(LEGO_IMAGES_DIR, filename)


# ── LEGO Sets ─────────────────────────────────────────────────────────────────
@bp.get("/lego-sets")
@login_required
def lego_sets():
    from datetime import datetime, timezone
    session = get_session()
    page = _page()
    try:
        year_filter = request.args.get("year", "")
        current_year = datetime.now(timezone.utc).year
        available_years = list(range(current_year - 5, current_year + 2))

        if year_filter:
            rows = session.execute(
                "SELECT year, item_no, description, image_url, first_seen, last_seen "
                "FROM lego_sets WHERE year = %s",
                (int(year_filter),),
            )
        else:
            # Fetch all years and combine
            all_rows = []
            for y in available_years:
                r = session.execute(
                    "SELECT year, item_no, description, image_url, first_seen, last_seen "
                    "FROM lego_sets WHERE year = %s",
                    (y,),
                )
                all_rows.extend(r)
            rows = all_rows

        data, total = _paginate(rows, page)
        error = None
    except Exception as exc:
        data, total, error = [], 0, str(exc)
        available_years = []
        year_filter = ""

    return render_template(
        "admin/lego_sets.html",
        rows=data,
        page=page,
        total=total,
        page_size=PAGE_SIZE,
        year_filter=year_filter,
        available_years=available_years,
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


# ── JSON API endpoints ────────────────────────────────────────────────────────
def _fmt_ts(ts) -> str:
    if ts is None:
        return ""
    return str(ts)[:19].replace("T", " ")


@bp.get("/api/stats")
@login_required
def api_stats():
    session = get_session()
    counts = {}
    for table in ("users", "items", "proxies", "golden_proxies", "sessions", "lego_sets"):
        try:
            row = session.execute(f"SELECT COUNT(*) FROM {table}").one()  # noqa: S608
            counts[table] = int(row[0])
        except Exception:
            counts[table] = None
    return jsonify(counts)


@bp.get("/api/proxies")
@login_required
def api_proxies():
    session = get_session()
    page = _page()
    protocol = request.args.get("protocol", "")
    try:
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
        return jsonify({
            "total": total,
            "rows": [
                {
                    "protocol": r.protocol,
                    "ip": r.ip,
                    "port": r.port,
                    "source": r.source or "",
                    "is_active": bool(r.is_active),
                    "first_seen": _fmt_ts(r.first_seen),
                    "last_seen": _fmt_ts(r.last_seen),
                }
                for r in data
            ],
        })
    except Exception as exc:
        return jsonify({"error": str(exc), "rows": [], "total": 0})


@bp.get("/api/golden-proxies")
@login_required
def api_golden_proxies_json():
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
        return jsonify({
            "total": total,
            "rows": [
                {
                    "protocol": r.protocol,
                    "ip": r.ip,
                    "port": r.port,
                    "source": r.source or "",
                    "first_seen": _fmt_ts(r.first_seen),
                    "last_seen": _fmt_ts(r.last_seen),
                }
                for r in data
            ],
        })
    except Exception as exc:
        return jsonify({"error": str(exc), "rows": [], "total": 0})


@bp.get("/api/lego-sets")
@login_required
def api_lego_sets_json():
    from datetime import datetime, timezone
    session = get_session()
    page = _page()
    year_filter = request.args.get("year", "")
    current_year = datetime.now(timezone.utc).year
    available_years = list(range(current_year - 5, current_year + 2))
    try:
        if year_filter:
            rows = session.execute(
                "SELECT year, item_no, description, image_url, first_seen, last_seen "
                "FROM lego_sets WHERE year = %s",
                (int(year_filter),),
            )
        else:
            all_rows = []
            for y in available_years:
                r = session.execute(
                    "SELECT year, item_no, description, image_url, first_seen, last_seen "
                    "FROM lego_sets WHERE year = %s",
                    (y,),
                )
                all_rows.extend(r)
            rows = all_rows
        data, total = _paginate(rows, page)
        return jsonify({
            "total": total,
            "rows": [
                {
                    "year": r.year,
                    "item_no": r.item_no,
                    "description": r.description or "",
                    "image_url": r.image_url or "",
                    "first_seen": _fmt_ts(r.first_seen),
                    "last_seen": _fmt_ts(r.last_seen),
                }
                for r in data
            ],
        })
    except Exception as exc:
        return jsonify({"error": str(exc), "rows": [], "total": 0})


@bp.get("/api/containers")
@login_required
def api_containers_json():
    try:
        import docker as docker_sdk
        client = docker_sdk.from_env()
        node_map = {}
        try:
            for n in client.nodes.list():
                desc = n.attrs.get("Description", {})
                hostname = desc.get("Hostname", n.short_id)
                addr = n.attrs.get("Status", {}).get("Addr", "")
                if not addr or addr == "0.0.0.0":
                    mgr_addr = n.attrs.get("ManagerStatus", {}).get("Addr", "")
                    addr = mgr_addr.split(":")[0] if mgr_addr else ""
                node_map[n.id] = f"{hostname} ({addr})" if addr else hostname
        except Exception:
            pass
        svc_map = {}
        try:
            for s in client.services.list():
                svc_map[s.id] = s.name
        except Exception:
            pass
        rows = []
        if node_map:
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
                    image = spec.get("ContainerSpec", {}).get("Image", "")
                    if "@" in image:
                        image = image.split("@")[0]
                    svc_id = t.get("ServiceID", "")
                    svc_name = svc_map.get(svc_id, svc_id[:12] if svc_id else "—")
                    slot = t.get("Slot")
                    name = f"{svc_name}.{slot}" if slot else svc_name
                    node_id = t.get("NodeID", "")
                    node_label = node_map.get(node_id, node_id[:12] if node_id else "—")
                    cid = ((t.get("Status") or {}).get("ContainerStatus") or {}).get("ContainerID", "")
                    rows.append({
                        "id": cid[:12] if cid else "—",
                        "name": name,
                        "image": image,
                        "status": state,
                        "node": node_label,
                        "created": t.get("CreatedAt", "")[:19].replace("T", " "),
                        "ports": "—",
                    })
                except Exception:
                    pass
            rows.sort(key=lambda r: (r["node"], r["name"]))
        else:
            for c in client.containers.list(all=False):
                try:
                    ports = []
                    for cp, bindings in (c.ports or {}).items():
                        if bindings:
                            for b in bindings:
                                ports.append(f"{b['HostPort']}→{cp}")
                        else:
                            ports.append(cp)
                    rows.append({
                        "id": c.short_id,
                        "name": c.name,
                        "image": (c.attrs.get("Config") or {}).get("Image") or c.short_id,
                        "status": c.status,
                        "node": "local",
                        "created": c.attrs.get("Created", "")[:19].replace("T", " "),
                        "ports": ", ".join(ports) or "—",
                    })
                except Exception:
                    pass
        return jsonify({"rows": rows, "swarm_mode": bool(node_map), "node_count": len(node_map), "error": None})
    except Exception as exc:
        return jsonify({"rows": [], "swarm_mode": False, "node_count": 0, "error": str(exc)})


# ── LEGO image backfill ───────────────────────────────────────────────────────
@bp.post("/api/backfill-lego-images")
@login_required
def api_backfill_lego_images():
    global _backfill_state
    with _backfill_lock:
        if _backfill_state["running"]:
            return jsonify({"status": "already_running", **_backfill_state})
    # collect all rows with an image URL
    from datetime import datetime, timezone
    session = get_session()
    current_year = datetime.now(timezone.utc).year
    all_rows = []
    for y in range(2010, current_year + 2):
        try:
            r = session.execute(
                "SELECT item_no, image_url FROM lego_sets WHERE year = %s", (y,)
            )
            all_rows.extend(r)
        except Exception:
            pass
    rows_with_url = [r for r in all_rows if r.image_url]
    with _backfill_lock:
        _backfill_state = {
            "running": True,
            "done": 0,
            "total": len(rows_with_url),
            "errors": 0,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
    t = threading.Thread(target=_backfill_worker, args=(rows_with_url,), daemon=True)
    t.start()
    return jsonify({"status": "started", **_backfill_state})


@bp.get("/api/backfill-lego-images")
@login_required
def api_backfill_lego_images_status():
    with _backfill_lock:
        return jsonify({"status": "running" if _backfill_state["running"] else "idle", **_backfill_state})
