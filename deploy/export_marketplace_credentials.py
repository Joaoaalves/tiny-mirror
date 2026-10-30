#!/usr/bin/env python3
"""Export marketplace credentials owned by OpenClaw to a file tiny-mirror can read.

OpenClaw's offshop-continuity plugin OWNS the marketplace integrations: it
stores the Amazon SP-API LWA credentials and the Shopee apps + shop tokens
under /root/.openclaw/credentials/marketplaces (root-only) and refreshes the
Shopee token hourly (Shopee refresh tokens rotate, so only one system may
refresh). tiny-mirror only READS a copy of what it needs:

- amazon_spapi: client_id, client_secret, refresh_token, seller_id,
  marketplace_id (LWA refresh tokens do not rotate; tiny-mirror mints its own
  short-lived access tokens)
- shopee_seller: partner_id, partner_key, shop_id, access_token, expires_at
  (never refreshed by tiny-mirror)
- tiktok_shop: app_key, app_secret, shop_id, shop_cipher, access_token,
  expires_at (the plugin renews the token at :14; tiny-mirror never refreshes
  it and never sees the refresh token). Optional: skipped until the shop is
  authorized.
- magalu: access_token, expires_at, seller_id, tenant_id, channel_id (the
  plugin renews the 2h token at :15/:45 — the ID Magalu refresh token ROTATES,
  so tiny-mirror never refreshes and never sees it nor the client secret).
  Optional: skipped until the store is authorized.

Install (root, on the VPS) — never run it from the deploy dir, which the
deploy user can write:

    install -o root -g root -m 755 deploy/export_marketplace_credentials.py /usr/local/bin/
    # root crontab: right after the Shopee (:12) and Magalu (:15/:45) refreshes
    17,47 * * * * /usr/local/bin/export_marketplace_credentials.py >> /var/log/export-marketplace-credentials.log 2>&1

Writes atomically to /opt/tiny-mirror/marketplace-credentials.json as
root:tinymirror 0640 (same pattern as ml_panel_cookiejar.txt).
"""

from __future__ import annotations

import grp
import json
import os
import sys
import tempfile
from datetime import UTC, datetime

SRC = "/root/.openclaw/credentials/marketplaces"
DST = "/opt/tiny-mirror/marketplace-credentials.json"
GROUP = "tinymirror"


def build() -> dict[str, object]:
    out: dict[str, object] = {"exported_at": datetime.now(UTC).isoformat()}
    with open(os.path.join(SRC, "accounts.json")) as fh:
        amazon = json.load(fh).get("amazon_spapi") or {}
    keys = ("client_id", "client_secret", "refresh_token", "seller_id", "marketplace_id")
    if all(amazon.get(k) for k in keys):
        out["amazon_spapi"] = {k: amazon[k] for k in keys}
    with open(os.path.join(SRC, "shopee.json")) as fh:
        shopee = json.load(fh)
    app = (shopee.get("apps") or {}).get("seller") or {}
    shop = next(
        (s for k, s in (shopee.get("shops") or {}).items() if k.startswith("seller:")), None
    )
    if app.get("partner_id") and app.get("partner_key") and shop and shop.get("access_token"):
        out["shopee_seller"] = {
            "partner_id": int(app["partner_id"]),
            "partner_key": app["partner_key"],
            "shop_id": int(shop["shop_id"]),
            "access_token": shop["access_token"],
            # plugin stores milliseconds; export seconds
            "expires_at": int(shop["expires_at"]) // 1000,
        }
    tiktok = _tiktok_shop()
    if tiktok:
        out["tiktok_shop"] = tiktok
    magalu = _magalu()
    if magalu:
        out["magalu"] = magalu
    return out


def _tiktok_shop() -> dict[str, object] | None:
    try:
        with open(os.path.join(SRC, "tiktok_shop.json")) as fh:
            conf = json.load(fh)
    except FileNotFoundError:
        return None
    auth = conf.get("auth") or {}
    shop = next(iter(conf.get("shops") or []), None)
    if not (conf.get("app_key") and conf.get("app_secret") and auth.get("access_token")):
        return None
    if not (shop and shop.get("id") and shop.get("cipher")):
        return None
    return {
        "app_key": conf["app_key"],
        "app_secret": conf["app_secret"],
        "shop_id": str(shop["id"]),
        "shop_cipher": shop["cipher"],
        "access_token": auth["access_token"],
        # plugin stores milliseconds; export seconds
        "expires_at": int(auth["expires_at"]) // 1000,
    }


def _magalu() -> dict[str, object] | None:
    try:
        with open(os.path.join(SRC, "magalu.json")) as fh:
            conf = json.load(fh)
    except FileNotFoundError:
        return None
    auth = conf.get("auth") or {}
    if not (auth.get("access_token") and auth.get("expires_at")):
        return None
    seller = conf.get("seller") or {}
    return {
        "access_token": auth["access_token"],
        # plugin stores milliseconds; export seconds
        "expires_at": int(auth["expires_at"]) // 1000,
        "seller_id": seller.get("id") or conf.get("seller_id"),
        "tenant_id": seller.get("tenant_id") or conf.get("tenant_id"),
        "channel_id": seller.get("channel_id") or conf.get("channel_id"),
    }


def main() -> int:
    payload = build()
    gid = grp.getgrnam(GROUP).gr_gid
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(DST), prefix=".mpcred-")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(payload, fh)
        os.chown(tmp, 0, gid)
        os.chmod(tmp, 0o640)
        os.replace(tmp, DST)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
    print(
        f"{payload['exported_at']} exported:" f" {sorted(k for k in payload if k != 'exported_at')}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
