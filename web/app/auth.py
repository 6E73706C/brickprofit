import os

from flask_login import LoginManager, UserMixin
from werkzeug.security import check_password_hash, generate_password_hash

login_manager = LoginManager()
login_manager.login_view = "auth.login"
login_manager.login_message = "Please log in to access the admin panel."
login_manager.login_message_category = "warning"
login_manager.session_protection = "strong"


class AdminUser(UserMixin):
    """Single admin user loaded from environment variables."""

    id = "admin"

    def __init__(self):
        raw = os.environ.get("ADMIN_PASSWORD", "")
        # Accept either a pre-hashed value (pbkdf2/scrypt prefix) or plain text
        if raw.startswith(("pbkdf2:", "scrypt:", "argon2:")):
            self._hash = raw
        else:
            self._hash = generate_password_hash(raw, method="pbkdf2:sha256:600000")

        self.username = os.environ.get("ADMIN_USERNAME", "admin")

    def check_password(self, password: str) -> bool:
        return check_password_hash(self._hash, password)


_admin = None


def get_admin() -> AdminUser:
    global _admin
    if _admin is None:
        _admin = AdminUser()
    return _admin


@login_manager.user_loader
def load_user(user_id: str):
    if user_id == "admin":
        return get_admin()
    return None
