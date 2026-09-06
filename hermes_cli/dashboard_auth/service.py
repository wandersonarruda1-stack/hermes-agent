"""File-backed profile credentials for hosted-room service callers."""
from __future__ import annotations

import hmac
import os
from pathlib import Path
import re
import secrets
import stat

from hermes_cli.dashboard_auth.base import DashboardAuthProvider, TokenPrincipal
from hermes_constants import get_default_hermes_root

TOKEN_FILE = "hosted-room-service.token"
TICKET_ROUTE = "/api/auth/service-ws-ticket"
SCOPE = "hosted-rooms"
_PROFILE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
_TOKEN = re.compile(r"[A-Za-z0-9_-]{43,128}\Z")
SERVICE_METHODS = frozenset({"gateway.ping", "groups.capabilities", "groups.create", "groups.send"})


def service_profile(identity):
    if not isinstance(identity, dict) or identity.get("provider") != "service":
        return None
    name = identity.get("user_id")
    return name if isinstance(name, str) and _PROFILE.fullmatch(name) else None


def _read_token(path: Path) -> str:
    """Reject symlinks, non-regular files, foreign ownership and loose modes."""
    if os.name != "posix":
        return ""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(fd, "r") as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
                    or info.st_uid != os.geteuid() or info.st_nlink != 1
                    or not 0 < info.st_size <= 130):
                return ""
            value = stream.read(130).strip()
            if not _TOKEN.fullmatch(value) or len(set(value)) < 16:
                return ""
            return value
    except (OSError, UnicodeError):
        return ""


class ProfileServiceProvider(DashboardAuthProvider):
    name = "service"
    display_name = "Hosted-room profile service"
    supports_token = True
    supports_session = False
    token_paths = (TICKET_ROUTE,)

    def __init__(self, root: Path | None = None):
        # Capture the canonical installation root, never a per-turn override.
        self.root = Path(root) if root is not None else get_default_hermes_root()

    def verify_token(self, *, token: str):
        if not isinstance(token, str) or not _TOKEN.fullmatch(token):
            return None
        homes = {"default": self.root}
        try:
            homes.update({p.name: p for p in (self.root / "profiles").iterdir()
                          if p.name != "default" and _PROFILE.fullmatch(p.name)
                          and p.is_dir() and not p.is_symlink()})
        except OSError:
            pass
        matches = [name for name, home in homes.items()
                   if hmac.compare_digest(token, _read_token(home / TOKEN_FILE))]
        # Duplicate credentials cannot choose whichever profile was scanned first.
        if len(matches) != 1:
            return None
        return TokenPrincipal(principal=matches[0], provider=self.name, scopes=(SCOPE,))

    def start_login(self, **kwargs):
        raise NotImplementedError("service credentials have no interactive login")

    def complete_login(self, **kwargs):
        raise NotImplementedError("service credentials have no interactive login")

    def verify_session(self, **kwargs):
        return None

    def refresh_session(self, **kwargs):
        raise NotImplementedError("service credentials have no session refresh")

    def revoke_session(self, **kwargs):
        return None


def provision_token(profile: str, *, root: Path | None = None) -> Path:
    """Create a new credential exclusively; never print it or replace an old one."""
    if os.name != "posix":
        raise NotImplementedError("service credential files require POSIX permissions")
    if not _PROFILE.fullmatch(profile):
        raise ValueError("invalid profile")
    root = Path(root) if root is not None else get_default_hermes_root()
    home = root if profile == "default" else root / "profiles" / profile
    if not home.is_dir() or home.is_symlink():
        raise ValueError("profile home must already exist and not be a symlink")
    path = home / TOKEN_FILE
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(secrets.token_urlsafe(32) + "\n")
    return path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Provision a hosted-room service credential without displaying it")
    parser.add_argument("--profile", required=True)
    args = parser.parse_args()
    provision_token(args.profile)
    print("Service credential created (0600); value was not displayed.")
