"""Seed the initial Tiny OAuth2 token into the ``oauth_tokens`` table.

Run manually exactly once, before the first deploy, after performing the
Authorization Code flow in a browser to obtain access and refresh tokens.

Required environment variables:
    DATABASE_URL                       — same URL used by the app
    TINY_INITIAL_ACCESS_TOKEN          — access token from the OAuth flow
    TINY_INITIAL_REFRESH_TOKEN         — corresponding refresh token
    TINY_ACCESS_TOKEN_EXPIRES_AT       — ISO 8601 datetime with timezone
    TINY_REFRESH_TOKEN_EXPIRES_AT      — ISO 8601 datetime with timezone

The script never lets a missing variable raise an unhandled traceback — it
prints a friendly error and exits with code 1.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

REQUIRED_VARS = (
    "DATABASE_URL",
    "TINY_INITIAL_ACCESS_TOKEN",
    "TINY_INITIAL_REFRESH_TOKEN",
    "TINY_ACCESS_TOKEN_EXPIRES_AT",
    "TINY_REFRESH_TOKEN_EXPIRES_AT",
)


def _read_env() -> dict[str, str]:
    missing = [name for name in REQUIRED_VARS if not os.environ.get(name)]
    if missing:
        print(
            "ERROR: the following environment variables are required and not set:",
            file=sys.stderr,
        )
        for name in missing:
            print(f"  - {name}", file=sys.stderr)
        sys.exit(1)
    return {name: os.environ[name] for name in REQUIRED_VARS}


def _parse_iso(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        print(f"ERROR: {field} is not valid ISO 8601 ({exc}).", file=sys.stderr)
        sys.exit(1)
    if parsed.tzinfo is None:
        print(f"ERROR: {field} must include a timezone offset.", file=sys.stderr)
        sys.exit(1)
    return parsed


def _normalize_database_url(url: str) -> str:
    if url.startswith("postgresql+asyncpg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    print(
        "ERROR: DATABASE_URL must start with postgresql+asyncpg:// or postgresql://.",
        file=sys.stderr,
    )
    sys.exit(1)


def _confirm_overwrite(existing_token: str, expires_at: datetime) -> bool:
    truncated = existing_token[:10] + "…" if len(existing_token) > 10 else existing_token
    print(f"An OAuth token already exists (token={truncated}, expires_at={expires_at.isoformat()}).")
    answer = input("Overwrite it? [s/N]: ").strip().lower()
    return answer in {"s", "sim", "y", "yes"}


async def _run() -> None:
    env = _read_env()
    access_expires = _parse_iso(env["TINY_ACCESS_TOKEN_EXPIRES_AT"], "TINY_ACCESS_TOKEN_EXPIRES_AT")
    refresh_expires = _parse_iso(env["TINY_REFRESH_TOKEN_EXPIRES_AT"], "TINY_REFRESH_TOKEN_EXPIRES_AT")

    now = datetime.now(tz=UTC)
    if access_expires <= now:
        print(
            f"WARNING: access token is already expired ({access_expires.isoformat()}). "
            "Continuing anyway (useful for tests).",
            file=sys.stderr,
        )

    db_url = _normalize_database_url(env["DATABASE_URL"])
    engine = create_async_engine(db_url, pool_pre_ping=True)

    try:
        async with engine.begin() as conn:
            existing = (
                await conn.execute(
                    text(
                        "SELECT access_token, access_token_expires_at "
                        "FROM oauth_tokens ORDER BY id LIMIT 1"
                    )
                )
            ).first()
            if existing is not None:
                if not _confirm_overwrite(existing[0], existing[1]):
                    print("Aborted by user. No changes made.")
                    return
                await conn.execute(
                    text(
                        "UPDATE oauth_tokens SET "
                        "access_token = :access, "
                        "refresh_token = :refresh, "
                        "access_token_expires_at = :access_exp, "
                        "refresh_token_expires_at = :refresh_exp, "
                        "updated_at = NOW()"
                    ),
                    {
                        "access": env["TINY_INITIAL_ACCESS_TOKEN"],
                        "refresh": env["TINY_INITIAL_REFRESH_TOKEN"],
                        "access_exp": access_expires,
                        "refresh_exp": refresh_expires,
                    },
                )
            else:
                await conn.execute(
                    text(
                        "INSERT INTO oauth_tokens "
                        "(access_token, refresh_token, "
                        "access_token_expires_at, refresh_token_expires_at) "
                        "VALUES (:access, :refresh, :access_exp, :refresh_exp)"
                    ),
                    {
                        "access": env["TINY_INITIAL_ACCESS_TOKEN"],
                        "refresh": env["TINY_INITIAL_REFRESH_TOKEN"],
                        "access_exp": access_expires,
                        "refresh_exp": refresh_expires,
                    },
                )
        print(f"OAuth token seeded successfully (expires_at={access_expires.isoformat()}).")
    finally:
        await engine.dispose()


def main() -> None:
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
