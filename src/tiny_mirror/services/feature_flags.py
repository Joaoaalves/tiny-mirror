"""Centralised reader for runtime feature flags.

Every flag here is a boolean toggled via env var on the VPS, not via any
HTTP endpoint. Routers/services import :func:`is_enabled` instead of
reading ``settings`` directly so tests can monkey-patch one place and so
the call site name (``feature_flags.is_enabled("ml_promo_apply")``)
greps as the authoritative gate point.

The current shipment is intentionally trivial: a single thin wrapper.
We expect to grow to a small registry as Phase 3/4 land (per-cap
overrides, per-promo-type gates, etc.), so the lookup stays cheap and
mockable.
"""

from __future__ import annotations

from tiny_mirror.config import settings

# Known flags. Add a new entry here when introducing a flag; the dispatch
# in :func:`is_enabled` will pick it up. Keep the dict literal so static
# analysis catches typos in callers.
_FLAG_TO_SETTING = {
    "ml_promo_apply": "ml_promo_apply_enabled",
}


def is_enabled(flag_name: str) -> bool:
    """Return the current value of ``flag_name``.

    Unknown flags return ``False`` rather than raising so that adding a
    gate point ahead of the setting cannot crash production. Callers that
    care about typos should rely on the registry above + linters.
    """
    setting_attr = _FLAG_TO_SETTING.get(flag_name)
    if setting_attr is None:
        return False
    return bool(getattr(settings, setting_attr, False))


def public_state() -> dict[str, bool]:
    """Return a dict of every known flag and its current bool.

    Surfaced via ``GET /api/produtos/promocoes/config`` so the UI can
    render a 'simulação vs execução' banner. The endpoint is read-only;
    no flag can be flipped from here.
    """
    return {name: is_enabled(name) for name in _FLAG_TO_SETTING}
