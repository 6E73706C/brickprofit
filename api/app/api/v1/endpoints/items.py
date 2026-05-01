from fastapi import APIRouter, Depends
from cassandra.cluster import Session
from app.core.db import get_session
import uuid

router = APIRouter()


@router.get("/")
def list_items(session: Session = Depends(get_session)):
    rows = session.execute("SELECT id, name, created_at FROM items LIMIT 100")
    return [dict(row._asdict()) for row in rows]


@router.get("/{item_id}")
def get_item(item_id: uuid.UUID, session: Session = Depends(get_session)):
    row = session.execute(
        "SELECT id, name, created_at FROM items WHERE id = %s", [item_id]
    ).one()
    if not row:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Item not found")
    return dict(row._asdict())


@router.post("/", status_code=201)
def create_item(name: str, session: Session = Depends(get_session)):
    item_id = uuid.uuid4()
    session.execute(
        "INSERT INTO items (id, name, created_at) VALUES (%s, %s, toTimestamp(now()))",
        [item_id, name],
    )
    return {"id": item_id, "name": name}
