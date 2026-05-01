from fastapi import APIRouter

router = APIRouter()


@router.get("/")
def list_users():
    # TODO: implement user management
    return []


@router.get("/me")
def me():
    return {"user": "anonymous"}
