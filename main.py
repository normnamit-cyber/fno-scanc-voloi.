"""
F&O Scan C — Volume/Open-Interest Ratio on Next-OTM Strikes
--------------------------------------------------------------
Watches the "next OTM" Call + Put across ALL F&O stocks + indices
(same selection rule as Scan B: CE = first strike above spot, PE = first
strike below spot).

Fires the INSTANT a contract's today's cumulative volume divided by its
current open interest crosses VOL_OI_RATIO_THRESHOLD — the standard
"unusual activity" signal used by professional options-flow scanners,
here applied specifically to OTM strikes (which is where this signal is
most meaningful, per the research this was built from).

Runs continuously during market hours, but market hours (9:15am-3:30pm,
~6h15m) run slightly longer than GitHub Actions' free 6-hour job limit —
so this same script runs as TWO separate jobs (morning + afternoon, see
the workflow file), sharing a small "already alerted today" file so a
contract that crosses the line doesn't ping you twice.
"""

import os
import time
import socket
import json
import subprocess
import pyotp
import requests
from datetime import datetime, timedelta, timezone

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

socket.setdefaulttimeout(30)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

ANGEL_API_KEY = os.environ["ANGEL_API_KEY"]
ANGEL_CLIENT_CODE = os.environ["ANGEL_CLIENT_CODE"]
ANGEL_PIN = os.environ["ANGEL_PIN"]
ANGEL_TOTP_SECRET = os.environ["ANGEL_TOTP_SECRET"]

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

IST = timezone(timedelta(hours=5, minutes=30))
MARKET_CLOSE = (15, 30)
MAX_RUN_MINUTES = 200  # ~3h20m safety cap per job, well under GH Actions' 6h limit

VOL_OI_RATIO_THRESHOLD = 1.5   # tune this once we see real data
MIN_VOLUME = 500               # ignore contracts too thin to matter
MIN_OI = 200

ALERTED_FILE = "data/scanc_alerted.json"
INSTRUMENT_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

# ---------------------------------------------------------------------------
# LOGIN
# ---------------------------------------------------------------------------

def login():
    print("[info] Attempting Angel One login...")
    try:
        smart_api = SmartConnect(api_key=ANGEL_API_KEY)
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        data = smart_api.generateSession(ANGEL_CLIENT_CODE, ANGEL_PIN, totp)
    except (socket.timeout, requests.exceptions.RequestException) as e:
        print(f"[error] Login request timed out or failed at the network level: {e}")
        raise
    if not data.get("status"):
        raise RuntimeError(f"Angel One login failed: {data}")
    auth_token = data["data"]["jwtToken"]
    feed_token = smart_api.getfeedToken()
    print("[info] Logged in to Angel One successfully.")
    return smart_api, auth_token, feed_token


# ---------------------------------------------------------------------------
# INSTRUMENT MASTER + WATCHLIST (same next-OTM rule as Scan B)
# ---------------------------------------------------------------------------

def load_instrument_master():
    print("[info] Downloading instrument master...")
    r = requests.get(INSTRUMENT_MASTER_URL, timeout=60)
    r.raise_for_status()
    return r.json()


def get_all_fo_underlyings(instrument_master):
    names = set(
        r["name"] for r in instrument_master
        if r.get("exch_seg") == "NFO" and r.get("instrumenttype") in ("OPTSTK", "OPTIDX")
    )
    return sorted(names)


def get_spot_ltp(smart_api, name, exch_seg, token):
    try:
        data = smart_api.ltpData(exch_seg, name, token)
        return float(data["data"]["ltp"])
    except Exception as e:
        print(f"[warn] Could not fetch LTP for {name}: {e}")
        return None


def nearest_expiry(option_rows):
    expiries = sorted(set(r["expiry"] for r in option_rows if r.get("expiry")))
    return expiries[0] if expiries else None


def build_watchlist(instrument_master, smart_api):
    watch_tokens = {}
    underlyings = get_all_fo_underlyings(instrument_master)
    print(f"[info] Discovered {len(underlyings)} F&O underlyings.")

    start_time = time.time()
    for idx, name in enumerate(underlyings, start=1):
        if idx % 10 == 0 or idx == 1:
            print(f"[info] Building watchlist... {idx}/{len(underlyings)} ({name})")
        if time.time() - start_time > 240:
            print(f"[warn] Time budget exceeded — stopping early at {idx}/{len(underlyings)}.")
            break

        option_rows = [
            r for r in instrument_master
            if r.get("name") == name and r.get("exch_seg") == "NFO"
            and r.get("instrumenttype") in ("OPTIDX", "OPTSTK")
        ]
        if not option_rows:
            continue

        expiry = nearest_expiry(option_rows)
        chain = [r for r in option_rows if r["expiry"] == expiry]

        underlying_row = next(
            (r for r in instrument_master
             if r.get("name") == name and r.get("exch_seg") in ("NSE", "NFO")
             and r.get("instrumenttype", "") in ("", "AMXIDX", "INDEX")),
            None,
        )
        if not underlying_row:
            continue

        spot = get_spot_ltp(smart_api, underlying_row["symbol"], underlying_row["exch_seg"], underlying_row["token"])
        if spot is None:
            continue

        strikes = sorted(set(float(r["strike"]) / 100 for r in chain))
        if not strikes:
            continue

        strikes_above = [s for s in strikes if s > spot]
        strikes_below = [s for s in strikes if s < spot]
        ce_strike = min(strikes_above) if strikes_above else None
        pe_strike = max(strikes_below) if strikes_below else None
        if ce_strike is None and pe_strike is None:
            continue

        for r in chain:
            strike_val = float(r["strike"]) / 100
            is_ce = strike_val == ce_strike and r["symbol"].endswith("CE")
            is_pe = strike_val == pe_strike and r["symbol"].endswith("PE")
            if is_ce or is_pe:
                watch_tokens[r["token"]] = {
                    "tradingsymbol": r["symbol"], "underlying": name,
                    "strike": strike_val, "type": "CE" if is_ce else "PE",
                }

    print(f"[info] Total contracts in watchlist: {len(watch_tokens)}")
    return watch_tokens


# ---------------------------------------------------------------------------
# ALERTED-TODAY STATE (shared between the morning and afternoon job)
# ---------------------------------------------------------------------------

def load_alerted_today(today_str):
    if os.path.exists(ALERTED_FILE):
        with open(ALERTED_FILE) as f:
            data = json.load(f)
        if data.get("date") == today_str:
            return set(data.get("tokens", []))
    return set()  # fresh for a new day


def save_alerted_today(today_str, alerted_set):
    os.makedirs("data", exist_ok=True)
    with open(ALERTED_FILE, "w") as f:
        json.dump({"date": today_str, "tokens": sorted(alerted_set)}, f)


def commit_alerted_state():
    """Push the updated dedup file back to the repo so the afternoon job
    (a separate GitHub Actions run) can see what the morning job already
    alerted on. Silently no-ops if git isn't configured for some reason —
    worst case, a contract could alert twice rather than the job crashing."""
    try:
        subprocess.run(["git", "config", "user.name", "scanc-bot"], check=True)
        subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)
        subprocess.run(["git", "add", ALERTED_FILE], check=True)
        result = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if result.returncode != 0:  # there are changes to commit
            subprocess.run(["git", "commit", "-m", "Update Scan C alerted state [skip ci]"], check=True)
            subprocess.run(["git", "pull", "--rebase"], check=True)
            subprocess.run(["git", "push"], check=True)
    except Exception as e:
        print(f"[warn] Could not commit alerted-state file: {e}")


# ---------------------------------------------------------------------------
# ALERTS
# ---------------------------------------------------------------------------

def send_scanc_alert(info, volume, oi, ratio):
    text = (
        f"*🔥 Scan C — Vol/OI Ratio Alert*\n"
        f"{info['underlying']} {info['strike']} {info['type']}\n"
        f"Symbol: {info['tradingsymbol']}\n"
        f"Today's Volume: {volume} | OI: {oi} | Ratio: {ratio:.2f}x"
    )
    send_telegram(text)
    print(f"[alert] {info['tradingsymbol']}: vol={volume} oi={oi} ratio={ratio:.2f}")


def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID, "text": text,
            "parse_mode": "Markdown", "disable_web_page_preview": True,
        }, timeout=10)
        if resp.status_code != 200:
            print(f"[error] Telegram send failed: {resp.text}")
    except Exception as e:
        print(f"[error] Telegram send exception: {e}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def stop_feed(sws):
    """Close the WebSocket connection. Tries the documented method first;
    falls back to a couple of alternates in case the exact method name
    ever drifts between SmartApi versions, so this can't silently fail
    the way it just did."""
    for attempt in ("close_connection", "close"):
        try:
            getattr(sws, attempt)()
            print(f"[info] Feed closed via sws.{attempt}().")
            return
        except AttributeError:
            continue
        except Exception as e:
            print(f"[warn] sws.{attempt}() raised: {e}")
            return
    print("[warn] Could not find a working close method on sws — "
          "the job will still end naturally once GitHub's timeout hits.")


def main():
    job_start = datetime.now(IST)
    today_str = job_start.strftime("%Y-%m-%d")

    smart_api, auth_token, feed_token = login()
    instrument_master = load_instrument_master()
    contracts = build_watchlist(instrument_master, smart_api)

    if not contracts:
        print("[error] Watchlist is empty — nothing to monitor.")
        return

    alerted_today = load_alerted_today(today_str)
    print(f"[info] {len(alerted_today)} contract(s) already alerted earlier today (skipping those).")

    tokens = list(contracts.keys())
    token_list = [{"exchangeType": 2, "tokens": tokens}]
    sws = SmartWebSocketV2(auth_token, ANGEL_API_KEY, ANGEL_CLIENT_CODE, feed_token)

    def should_stop(now):
        close_t = now.replace(hour=MARKET_CLOSE[0], minute=MARKET_CLOSE[1], second=0, microsecond=0)
        elapsed_min = (now - job_start).total_seconds() / 60
        return now >= close_t or elapsed_min >= MAX_RUN_MINUTES

    def on_data(wsapp, message):
        try:
            token = message.get("token")
            if token is None or token not in contracts or token in alerted_today:
                return
            # NOTE: field names below are our best expectation for Angel One's
            # full-quote (mode 3) payload. If the watchlist builds fine but no
            # ratios ever compute, this is the first thing to verify against
            # a real captured payload — Angel One's exact key names have
            # drifted between versions before.
            volume = message.get("volume_trade_for_the_day") or message.get("volume")
            oi = message.get("open_interest") or message.get("opnInterest")
            if volume and oi and oi >= MIN_OI and volume >= MIN_VOLUME:
                ratio = volume / oi
                if ratio >= VOL_OI_RATIO_THRESHOLD:
                    alerted_today.add(token)
                    send_scanc_alert(contracts[token], volume, oi, ratio)

            now = datetime.now(IST)
            if should_stop(now):
                stop_feed(sws)
        except Exception as e:
            print(f"[warn] on_data error: {e}")

    def on_open(wsapp):
        print("[info] WebSocket connected, subscribing...")
        sws.subscribe("scanc-bot", 3, token_list)

    def on_error(wsapp, error):
        print(f"[error] WebSocket error: {error}")

    def on_close(wsapp):
        print("[info] WebSocket closed.")

    sws.on_open = on_open
    sws.on_data = on_data
    sws.on_error = on_error
    sws.on_close = on_close

    print(f"[info] Starting live feed — will run until market close or {MAX_RUN_MINUTES} min, whichever first.")
    send_telegram(f"✅ Scan C bot started this session — watching {len(contracts)} next-OTM contracts.")

    sws.connect()  # blocks until closed

    save_alerted_today(today_str, alerted_today)
    commit_alerted_state()
    print("[info] Session ended.")


if __name__ == "__main__":
    main()
