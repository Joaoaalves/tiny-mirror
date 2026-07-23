#!/usr/bin/env python3
"""Dispatcher de alertas de galpao_anomalies (roda por cron na VPS, como root).

Lê as linhas ainda não notificadas, envia UMA mensagem agrupada e marca
notified_at. Canal controlado por env:

  GALPAO_ALERT_CHANNEL=telegram   (default) → Telegram Bot API direto
      TELEGRAM_BOT_TOKEN + GALPAO_ALERT_CHAT_ID (default 1084709545)
  GALPAO_ALERT_CHANNEL=whatsapp             → `openclaw message send`
      GALPAO_ALERT_WHATSAPP_TO=<numero E.164, ex: +5511999999999>

Banco via DATABASE_URL (postgresql://...) ou padrão local do espelho.
Idempotente: marca notified_at ANTES de considerar a execução ok apenas se
o envio retornou sucesso; em falha, as linhas ficam para o próximo ciclo.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request

import psycopg2

DB_DSN = os.environ.get(
    "GALPAO_ALERT_DSN",
    "dbname=tiny_mirror_db user=postgres host=/var/run/postgresql",
)
CHANNEL = os.environ.get("GALPAO_ALERT_CHANNEL", "telegram")
CHAT_ID = os.environ.get("GALPAO_ALERT_CHAT_ID", "1084709545")
WHATSAPP_TO = os.environ.get("GALPAO_ALERT_WHATSAPP_TO", "")
MAX_LINES = 20


def fetch_pending(cur) -> list[tuple]:
    cur.execute(
        """
        SELECT id, sku, prev_galpao, new_galpao, delta, reason,
               to_char(detected_at AT TIME ZONE 'America/Sao_Paulo', 'DD/MM HH24:MI')
        FROM galpao_anomalies
        WHERE notified_at IS NULL
        ORDER BY detected_at
        LIMIT 100
        """
    )
    return cur.fetchall()


def build_message(rows: list[tuple]) -> str:
    lines = [f"⚠️ GALPÃO: {len(rows)} movimentação(ões) suspeita(s)"]
    for _id, sku, prev, new, delta, reason, ts in rows[:MAX_LINES]:
        lines.append(f"• {ts}  {sku}: {prev} → {new} ({delta:+d})  [{reason}]")
    if len(rows) > MAX_LINES:
        lines.append(f"… e mais {len(rows) - MAX_LINES}.")
    lines.append("Confira no Tiny o histórico ANTES que role a página de movimentos.")
    return "\n".join(lines)


def send_telegram(text: str) -> bool:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        print("TELEGRAM_BOT_TOKEN ausente", file=sys.stderr)
        return False
    body = urllib.parse.urlencode({"chat_id": CHAT_ID, "text": text}).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage", data=body, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return 200 <= resp.status < 300
    except Exception as exc:
        print(f"telegram send falhou: {exc}", file=sys.stderr)
        return False


def send_whatsapp(text: str) -> bool:
    if not WHATSAPP_TO:
        print("GALPAO_ALERT_WHATSAPP_TO ausente", file=sys.stderr)
        return False
    r = subprocess.run(
        [
            "openclaw",
            "message",
            "send",
            "--channel",
            "whatsapp",
            "--target",
            WHATSAPP_TO,
            "--message",
            text,
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if r.returncode != 0:
        print(f"openclaw send falhou: {r.stderr[:300]}", file=sys.stderr)
    return r.returncode == 0


def main() -> int:
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False
    cur = conn.cursor()
    rows = fetch_pending(cur)
    if not rows:
        print(json.dumps({"pending": 0}))
        return 0
    msg = build_message(rows)
    ok = send_whatsapp(msg) if CHANNEL == "whatsapp" else send_telegram(msg)
    if ok:
        cur.execute(
            "UPDATE galpao_anomalies SET notified_at = now() WHERE id = ANY(%s)",
            ([r[0] for r in rows],),
        )
        conn.commit()
    print(json.dumps({"pending": len(rows), "sent": ok, "channel": CHANNEL}))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
