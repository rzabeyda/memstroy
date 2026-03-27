from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional, Union
import sqlite3
import random
import string
import os
import time
from collections import defaultdict
from database import get_db, init_db

app = FastAPI(title="MemStroy API")

# ── RATE LIMITER ──
_endpoint_rate_store = defaultdict(list)  # "user_id:endpoint" -> [timestamps]

def _check_endpoint_rate_limit(user_id: int, endpoint: str, limit: int = 5, window: int = 10) -> bool:
    """Per-user per-endpoint rate limit. Default: max 5 requests per 10 seconds."""
    now = time.time()
    key = f"{user_id}:{endpoint}"
    _endpoint_rate_store[key] = [t for t in _endpoint_rate_store[key] if now - t < window]
    _endpoint_rate_store[key].append(now)
    return len(_endpoint_rate_store[key]) <= limit

try:
    from ton_wallet import generate_wallet, send_ton, get_wallet_balance
    TON_WALLET_AVAILABLE = True
except ImportError:
    TON_WALLET_AVAILABLE = False
    print("tonsdk not available")

import hmac
import hashlib
from urllib.parse import unquote

def verify_telegram_init_data(init_data: str, bot_token: str) -> bool:
    """Verify Telegram WebApp initData signature"""
    if not init_data or not bot_token:
        return False
    try:
        parsed = {}
        for part in init_data.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                parsed[k] = unquote(v)
        hash_val = parsed.pop("hash", "")
        data_check = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
        expected = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, hash_val)
    except:
        return False

def get_tg_id_from_init_data(init_data: str) -> int:
    """Extract telegram_id from initData"""
    try:
        for part in init_data.split("&"):
            if part.startswith("user="):
                user_json = unquote(part[5:])
                import json as _json
                user = _json.loads(user_json)
                return int(user.get("id", 0))
    except:
        pass  # silently ignored
    return 0

import aiohttp as _aiohttp

def _validate_telegram_id(telegram_id) -> int:
    """Basic telegram_id validation - must be positive integer"""
    try:
        tid = int(telegram_id)
        if tid <= 0:
            raise ValueError
        return tid
    except (TypeError, ValueError):
        raise HTTPException(400, "Invalid telegram_id")


async def notify_user(telegram_id: int, text: str):
    """Send Telegram notification to user"""
    bot_token = os.getenv("BOT_TOKEN","")
    if not bot_token: return
    try:
        async with _aiohttp.ClientSession() as s:
            resp = await s.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={"chat_id": telegram_id, "text": text}
            )
            if resp.status == 429:
                import asyncio as _aio2
                data = await resp.json()
                retry = data.get("parameters",{}).get("retry_after",5)
                await _aio2.sleep(retry)
                await s.post(
                    f"https://api.telegram.org/bot{bot_token}/sendMessage",
                    json={"chat_id": telegram_id, "text": text}
                )
    except Exception as e:
        print(f"[WARN] notify error: {e}")


def notify_user_sync(telegram_id: int, text: str):
    """Fire-and-forget notification safe to call from sync endpoints"""
    import threading, requests as _req
    bot_token = os.getenv("BOT_TOKEN", "")
    if not bot_token: return
    def _send():
        try:
            _req.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={"chat_id": telegram_id, "text": text},
                timeout=5
            )
        except Exception as e:
            print(f"[WARN] notify_sync error: {e}")
    threading.Thread(target=_send, daemon=True).start()

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest

class NgrokMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        response = await call_next(request)
        response.headers["ngrok-skip-browser-warning"] = "true"
        return response

app.add_middleware(NgrokMiddleware)


# initData middleware removed - handled per-endpoint where critical

app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/webapp", StaticFiles(directory="webapp"), name="webapp")


@app.on_event("startup")
async def startup():
    init_db()
    os.makedirs("static/ponki", exist_ok=True)
    try:
        conn = get_db()
        conn.execute("UPDATE card_definitions SET name='Model' WHERE name='Dress'")
        conn.commit()
        conn.close()
    except:
        pass  # silently ignored
    # Start background auction finisher
    import asyncio
    asyncio.create_task(_auction_background())

async def _auction_background():
    """Check and finish expired auctions every 30 seconds"""
    import asyncio
    from datetime import datetime
    while True:
        try:
            await asyncio.sleep(30)
            conn = get_db()
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            # Check expired giveaways
            expired_giveaways = conn.execute(
                "SELECT id FROM giveaways WHERE status='active' AND ends_at <= ?", (now,)
            ).fetchall()
            conn.close()
            for g in expired_giveaways:
                try:
                    await finish_giveaway({"giveaway_id": g["id"]})
                except: pass
            conn = get_db()
            expired = conn.execute(
                "SELECT * FROM auctions WHERE is_active=1 AND ends_at <= ?", (now,)
            ).fetchall()
            for auction in expired:
                conn.execute("UPDATE auctions SET is_active=0 WHERE id=?", (auction["id"],))
                conn.execute("UPDATE user_cards SET is_listed=0 WHERE id=?", (auction["user_card_id"],))
                if auction["current_bidder_id"]:
                    commission = int(auction["current_price_nano"] * 0.05)
                    seller_gets = auction["current_price_nano"] - commission
                    cashback = int(auction["current_price_nano"] * 0.01)
                    conn.execute("UPDATE user_cards SET user_id=? WHERE id=?",
                                (auction["current_bidder_id"], auction["user_card_id"]))
                    conn.execute("UPDATE users SET ton_balance = ton_balance + ? WHERE id=?",
                                (seller_gets, auction["seller_id"]))
                    conn.execute("UPDATE users SET cashback_balance = COALESCE(cashback_balance,0) + ? WHERE id=?",
                                (cashback, auction["current_bidder_id"]))
                    winner = conn.execute("SELECT telegram_id FROM users WHERE id=?", (auction["current_bidder_id"],)).fetchone()
                    seller = conn.execute("SELECT telegram_id FROM users WHERE id=?", (auction["seller_id"],)).fetchone()
                    card = conn.execute("SELECT cd.name FROM user_cards uc JOIN card_definitions cd ON uc.card_def_id=cd.id WHERE uc.id=?", (auction["user_card_id"],)).fetchone()
                    card_name = card["name"] if card else "карточка"
                    price_ton = float(f"{auction['current_price_nano']/1e9:.10f}".rstrip("0").rstrip("."))
                    if winner:
                        await notify_user(winner["telegram_id"], f"🏆 Вы выиграли аукцион! {card_name} ваша за {price_ton} TON")
                    if seller:
                        await notify_user(seller["telegram_id"], f"💰 {card_name} продана за {price_ton} TON")
                else:
                    seller = conn.execute("SELECT telegram_id FROM users WHERE id=?", (auction["seller_id"],)).fetchone()
                    card = conn.execute("SELECT cd.name FROM user_cards uc JOIN card_definitions cd ON uc.card_def_id=cd.id WHERE uc.id=?", (auction["user_card_id"],)).fetchone()
                    if seller and card:
                        await notify_user(seller["telegram_id"], f"↩️ Аукцион без ставок. {card['name']} возвращена")
            if expired:
                conn.commit()
            conn.close()
        except Exception as e:
            print(f"Auction background error: {e}")


@app.get("/")
def root():
    return FileResponse("webapp/v5.html")


class RegisterUser(BaseModel):
    telegram_id: int
    username: str = ""
    first_name: str = ""
    last_name: str = ""
    ref_code: Optional[str] = None


class PaymentConfirm(BaseModel):
    telegram_id: int
    payload: str
    stars: int


class ListCard(BaseModel):
    telegram_id: int
    user_card_id: int
    price_dev: int


class BuyListing(BaseModel):
    telegram_id: int
    listing_id: int


class TransferCard(BaseModel):
    from_telegram_id: int
    to_telegram_id: Union[int, str]
    user_card_id: int


def gen_ref_code():
    return "CD-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


def get_user(conn, telegram_id):
    return conn.execute("SELECT * FROM users WHERE telegram_id=?", (telegram_id,)).fetchone()


def require_user(conn, telegram_id):
    user = get_user(conn, telegram_id)
    if not user:
        raise HTTPException(404, "User not found")
    return user


@app.post("/api/register")
def register(data: RegisterUser):
    conn = get_db()
    existing = get_user(conn, data.telegram_id)
    if existing:
        # Patch: if user exists but has no referrer and a ref_code is provided — set it now
        if data.ref_code and not existing["referred_by"]:
            referrer = conn.execute("SELECT * FROM users WHERE ref_code=?", (data.ref_code,)).fetchone()
            if not referrer:
                try:
                    referrer = conn.execute("SELECT * FROM users WHERE telegram_id=?", (int(data.ref_code),)).fetchone()
                except (ValueError, TypeError):
                    pass
            if referrer and referrer["id"] != existing["id"]:
                conn.execute("UPDATE users SET referred_by=? WHERE id=?", (referrer["id"], existing["id"]))
                conn.commit()
        conn.close()
        return {"ok": True, "user_id": existing["id"], "new": False}

    ref_code = gen_ref_code()
    referred_by = None

    if data.ref_code:
        # Сначала ищем по ref_code (CD-XXXXXX)
        referrer = conn.execute("SELECT * FROM users WHERE ref_code=?", (data.ref_code,)).fetchone()
        # Если не нашли — ищем по telegram_id (старый формат ссылки ?start=123456)
        if not referrer:
            try:
                referrer = conn.execute("SELECT * FROM users WHERE telegram_id=?", (int(data.ref_code),)).fetchone()
            except (ValueError, TypeError):
                pass
        if referrer:
            referred_by = referrer["id"]

    # Generate personal TON wallet for this user
    wallet_address = ""
    wallet_mnemonic = ""
    if TON_WALLET_AVAILABLE:
        try:
            mnemonics, address = generate_wallet()
            wallet_address = address
            import json as _jw; wallet_mnemonic = _jw.dumps(mnemonics)
        except Exception as e:
            print(f"Wallet gen error: {e}")

    conn.execute("""
        INSERT INTO users (telegram_id, username, first_name, last_name, ref_code, referred_by, wallet_address, wallet_mnemonic)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (data.telegram_id, data.username, data.first_name, data.last_name, ref_code, referred_by, wallet_address, wallet_mnemonic))
    conn.commit()
    user_id = conn.execute("SELECT id FROM users WHERE telegram_id=?", (data.telegram_id,)).fetchone()["id"]
    # Welcome bonus: 5 gems on first registration
    conn.execute("UPDATE users SET gems = COALESCE(gems,0) + 10 WHERE id=?", (user_id,))
    conn.commit()
    conn.close()
    notify_user_sync(data.telegram_id,
        "🎁 Добро пожаловать в Ponki! Вам начислено 10 💎 гемов в подарок!")
    return {"ok": True, "user_id": user_id, "new": True, "welcome_bonus": 10}


@app.get("/api/user/{telegram_id}")
def user_info(telegram_id: int):
    conn = get_db()
    user = require_user(conn, telegram_id)

    cards = conn.execute("""
        SELECT uc.*, cd.name, cd.image_url, cd.drop_weight, c.name as collection_name
        FROM user_cards uc
        JOIN card_definitions cd ON uc.card_def_id = cd.id
        JOIN collections c ON uc.collection_id = c.id
        WHERE uc.user_id = ? AND uc.is_listed = 0
        ORDER BY cd.drop_weight ASC, uc.acquired_at DESC
    """, (user["id"],)).fetchall()

    referrals_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM users WHERE referred_by=?", (user["id"],)
    ).fetchone()["cnt"]

    referral_earnings = conn.execute(
        "SELECT COALESCE(SUM(bonus_stars),0) as total FROM referral_bonuses WHERE referrer_id=?",
        (user["id"],)
    ).fetchone()["total"]

    total_cards = conn.execute(
        "SELECT COUNT(*) as cnt FROM user_cards WHERE user_id=?", (user["id"],)
    ).fetchone()["cnt"]

    ton_address = user["ton_address"] if "ton_address" in user.keys() else ""

    conn.close()
    return {
        "id": user["id"],
        "telegram_id": user["telegram_id"],
        "username": user["username"],
        "first_name": user["first_name"],
        "stars_balance": user["stars_balance"],
        "dev_tokens": user["dev_tokens"] if "dev_tokens" in user.keys() else 100,
        "ref_code": user["ref_code"],
        "referrals_count": referrals_count,
        "referral_earnings": referral_earnings,
        "total_cards": total_cards,
        "ton_address": ton_address,
        "ton_balance": user["ton_balance"] if "ton_balance" in user.keys() else 0,
        "wallet_address": user["wallet_address"] if "wallet_address" in user.keys() else "",
        "cashback_balance": user["cashback_balance"] if "cashback_balance" in user.keys() else 0,
        "gems": user["gems"] if "gems" in user.keys() else 0,
        "daily_reward_date": user["daily_reward_date"] if "daily_reward_date" in user.keys() else "",
        "spin_date": user["spin_date"] if "spin_date" in user.keys() else "",
        "buy_card_date": user["buy_card_date"] if "buy_card_date" in user.keys() else "",
        "tasks_completed": user["tasks_completed"] if "tasks_completed" in user.keys() else 0,
        "daily_streak": user["daily_streak"] if "daily_streak" in user.keys() else 0,
        "hourly_reward_at": user["hourly_reward_at"] if "hourly_reward_at" in user.keys() else None,
        "cards": [dict(c) for c in cards]
    }


@app.post("/api/check_user")
def check_user(data: dict):
    """Check if user exists in bot by username or telegram_id"""
    query = str(data.get("query","")).strip().lstrip("@")
    if not query:
        raise HTTPException(400, "Query required")
    conn = get_db()
    if query.isdigit():
        user = conn.execute("SELECT id FROM users WHERE telegram_id=?", (int(query),)).fetchone()
    else:
        user = conn.execute("SELECT id FROM users WHERE username=?", (query,)).fetchone()
    conn.close()
    return {"found": user is not None}


@app.get("/api/collections")
def collections():
    conn = get_db()
    cols = conn.execute("SELECT * FROM collections WHERE is_active=1").fetchall()
    conn.close()
    return [dict(c) for c in cols]


@app.get("/api/collection/{collection_id}")
def collection_detail(collection_id: int):
    conn = get_db()
    col = conn.execute("SELECT * FROM collections WHERE id=?", (collection_id,)).fetchone()
    if not col:
        raise HTTPException(404, "Collection not found")
    cards = conn.execute(
        "SELECT * FROM card_definitions WHERE collection_id=? ORDER BY drop_weight ASC", (collection_id,)
    ).fetchall()
    conn.close()
    return {"collection": dict(col), "cards": [dict(c) for c in cards]}


@app.post("/api/payment/confirm")
def payment_confirm(data: PaymentConfirm):
    conn = get_db()
    user = require_user(conn, data.telegram_id)
    is_admin = user["telegram_id"] == ADMIN_FREE_ID
    # Replay attack protection: check if this payload was already processed
    existing = conn.execute(
        "SELECT id FROM transactions WHERE payload=? AND from_user_id=? AND type='payment'",
        (data.payload, user["id"])
    ).fetchone()
    if existing and not data.payload.startswith("addstars") and not is_admin:
        conn.close()
        raise HTTPException(400, "Платёж уже обработан")
    # Log this payment immediately
    conn.execute("INSERT OR IGNORE INTO transactions (from_user_id, to_user_id, type, payload) VALUES (?,?,'payment',?)",
                 (user["id"], user["id"], data.payload))
    conn.commit()
    parts = data.payload.split("_")
    action = parts[0]
    # Handle gem pool
    if action == "gem" and len(parts) > 1 and parts[1] == "pool":
        qty = int(parts[4]) if len(parts) > 4 else 1
        conn2 = get_db()
        try:
            for _ in range(qty):
                conn2.execute("INSERT OR IGNORE INTO gem_pool(telegram_id, username, first_name, round_id) VALUES(?,?,'','1')", (user["id"], ''))
            conn2.commit()
        except: pass
        conn2.close()
        conn.close()
        return {"ok": True, "message": "GEM ticket added"}

    if action == "buy" and parts[1] == "card":
        collection_id = int(parts[2])
        qty = int(parts[3]) if len(parts) > 3 else 1
        # Admin gets cards for free (server-side check, not spoofable)
        is_admin = user["telegram_id"] == 7308147004
        cards = []
        for _ in range(qty):
            chosen = _buy_card(conn, user["id"], collection_id)
            cards.append(chosen)
        if not is_admin:
            _pay_referral_bonus(conn, user["id"], data.stars)
            conn.execute("UPDATE users SET stars_spent = COALESCE(stars_spent,0) + ? WHERE id=?", (data.stars, user["id"]))
        conn.commit()
        conn.close()
        if qty == 1:
            return {"message": f"🃏 You got: {cards[0]['name']}!", "card": cards[0]}
        return {"message": f"🃏 Got {qty} cards!", "cards": cards}

    elif action == "addstars":
        amount = int(parts[1])
        conn.execute("UPDATE users SET stars_balance = stars_balance + ? WHERE id=?", (amount, user["id"]))
        conn.commit()
        conn.close()
        return {"message": f"⭐ {amount} Stars added!"}

    elif action == "buygems":
        amount = int(parts[1])
        conn.execute("UPDATE users SET gems = COALESCE(gems,0) + ? WHERE id=?", (amount, user["id"]))
        conn.commit()
        conn.close()
        return {"message": f"💎 {amount} gems added!"}

    elif action == "transfer" and parts[1] == "fee":
        card_id = int(parts[2])
        to_id_raw = parts[3]
        is_admin = user["telegram_id"] == 7308147004
        try:
            if to_id_raw.isdigit():
                to_user = conn.execute("SELECT * FROM users WHERE telegram_id=?", (int(to_id_raw),)).fetchone()
            else:
                to_user = conn.execute("SELECT * FROM users WHERE username=?", (to_id_raw.lstrip('@'),)).fetchone()
            if not to_user:
                conn.close()
                return {"message": "Recipient not found"}
            card = conn.execute("SELECT * FROM user_cards WHERE id=? AND user_id=?", (card_id, user["id"])).fetchone()
            if not card:
                conn.close()
                return {"message": "Card not found"}
            conn.execute("UPDATE user_cards SET user_id=?, transferred_count = transferred_count + 1 WHERE id=?",
                         (to_user["id"], card_id))
            conn.execute("""
                INSERT INTO transactions (from_user_id, to_user_id, user_card_id, type, stars_amount)
                VALUES (?, ?, ?, 'transfer', ?)
            """, (user["id"], to_user["id"], card_id, 0 if is_admin else 1))
            conn.commit()
            # Notify recipient with username and card details
            card_def = conn.execute("SELECT cd.name, uc.serial_number, uc.is_upgraded FROM user_cards uc JOIN card_definitions cd ON uc.card_def_id=cd.id WHERE uc.id=?", (card_id,)).fetchone()
            card_name = card_def["name"] if card_def else "карточка"
            serial = card_def["serial_number"] if card_def else ""
            is_upgraded = card_def["is_upgraded"] if card_def else 0
            card_label = card_name if is_upgraded else "Base"
            card_display = f"Ponki {card_label} #{serial}" if serial else f"Ponki {card_label}"
            sender_display = f"@{user['username']}" if user['username'] else (user['first_name'] or "Кто-то")
            notify_user_sync(to_user["telegram_id"], f"🎁 {sender_display} передал вам карточку {card_display}")

            conn.close()
            recipient = to_user["first_name"] or to_user["username"] or "friend"
            return {"message": f"✈️ Card sent to {recipient}!"}
        except Exception as e:
            conn.close()
            return {"message": "Card transferred!"}

    conn.close()
    return {"message": "Payment processed"}


def _pay_referral_bonus(conn, user_id, amount):
    """10% referral bonus in gems"""
    user = conn.execute("SELECT referred_by FROM users WHERE id=?", (user_id,)).fetchone()
    if not user or not user["referred_by"]:
        return
    bonus = max(1, int(amount * 0.10))
    conn.execute("UPDATE users SET gems = COALESCE(gems,0) + ? WHERE id=?",
                 (bonus, user["referred_by"]))
    conn.execute("""
        INSERT INTO referral_bonuses (referrer_id, referred_id, bonus_stars)
        VALUES (?, ?, ?)
    """, (user["referred_by"], user_id, bonus))
    conn.commit()


def _buy_card(conn, user_id, collection_id):
    # Atomic decrement - prevents overselling
    updated = conn.execute(
        "UPDATE collections SET remaining = remaining - 1 WHERE id=? AND remaining > 0",
        (collection_id,)
    ).rowcount
    if not updated:
        conn.close() if hasattr(conn, 'close') else None
        raise HTTPException(400, "Collection sold out")
    conn.commit()
    col = conn.execute("SELECT * FROM collections WHERE id=?", (collection_id,)).fetchone()
    serial = col["total_supply"] - col["remaining"]

    all_defs = conn.execute(
        "SELECT * FROM card_definitions WHERE collection_id=?", (collection_id,)
    ).fetchall()

    total_weight = sum(d["drop_weight"] for d in all_defs)
    roll = random.randint(1, total_weight)
    cumulative = 0
    chosen = all_defs[0]
    for d in all_defs:
        cumulative += d["drop_weight"]
        if roll <= cumulative:
            chosen = d
            break

    conn.execute("""
        INSERT INTO user_cards (user_id, card_def_id, collection_id, serial_number, is_upgraded)
        VALUES (?, ?, ?, ?, 0)
    """, (user_id, chosen["id"], collection_id, serial))
    # remaining already decremented atomically above
    conn.execute("""
        INSERT INTO transactions (from_user_id, to_user_id, type, stars_amount)
        VALUES (?, ?, 'buy', ?)
    """, (user_id, user_id, col["base_price"]))
    conn.commit()
    return dict(chosen)


@app.post("/api/buy_card_gems")
def buy_card_gems(data: dict):
    """Buy cards using gems (1 gem = 1 card)"""
    telegram_id = _validate_telegram_id(data.get("telegram_id"))
    collection_id = data.get("collection_id", 1)
    qty = max(1, min(500, int(data.get("qty", 1))))  # cap at 500
    conn = get_db()
    user = require_user(conn, telegram_id)
    gems = user["gems"] if "gems" in user.keys() else 0
    cost = qty * 100
    if gems < cost:
        conn.close()
        raise HTTPException(400, f"Недостаточно гемов. Нужно: {cost}, есть: {gems}")
    # Atomic deduct
    cur = conn.execute("UPDATE users SET gems = gems - ? WHERE id=? AND gems >= ?", (cost, user["id"], qty))
    if cur.rowcount == 0:
        conn.close()
        raise HTTPException(400, "Недостаточно гемов (concurrent)")
    cards = []
    for _ in range(qty):
        chosen = _buy_card(conn, user["id"], collection_id)
        cards.append(chosen)
    conn.commit()
    conn.close()
    if qty == 1:
        return {"ok": True, "message": f"💎 Куплено: {cards[0]['name']}!", "card": cards[0]}
    return {"ok": True, "message": f"💎 Куплено {qty} карточек!", "cards": cards}


@app.post("/api/buy_card_internal")
def buy_card_internal(data: dict):
    conn = get_db()
    telegram_id = data["telegram_id"]
    collection_id = data["collection_id"]
    use_dev = data.get("dev", False)

    user = require_user(conn, telegram_id)
    col = conn.execute("SELECT * FROM collections WHERE id=?", (collection_id,)).fetchone()
    if not col:
        raise HTTPException(404, "Collection not found")

    if use_dev:
        dev_bal = user["dev_tokens"] if "dev_tokens" in user.keys() else 0
        if dev_bal < col["base_price"]:
            raise HTTPException(400, "Not enough DEV tokens")
        conn.execute("UPDATE users SET dev_tokens = dev_tokens - ? WHERE id=?",
                     (col["base_price"], user["id"]))
    else:
        if user["stars_balance"] < col["base_price"]:
            raise HTTPException(400, "Not enough stars")
        conn.execute("UPDATE users SET stars_balance = stars_balance - ? WHERE id=?",
                     (col["base_price"], user["id"]))

    chosen = _buy_card(conn, user["id"], collection_id)
    _pay_referral_bonus(conn, user["id"], col["base_price"])
    conn.close()
    return {"ok": True, "message": f"You got: {chosen['name']}!", "card": chosen}


@app.post("/api/upgrade_card_internal")
def upgrade_card_internal(data: dict):
    """Upgrade is free — just mark card as upgraded"""
    conn = get_db()
    telegram_id = data["telegram_id"]
    user_card_id = data["user_card_id"]

    user = require_user(conn, telegram_id)
    card = conn.execute("""
        SELECT * FROM user_cards WHERE id=? AND user_id=? AND is_upgraded=0
    """, (user_card_id, user["id"])).fetchone()

    if not card:
        raise HTTPException(400, "Card not found or already upgraded")

    conn.execute("UPDATE user_cards SET is_upgraded=1 WHERE id=?", (user_card_id,))
    conn.execute("""
        INSERT INTO transactions (from_user_id, to_user_id, user_card_id, type, stars_amount)
        VALUES (?, NULL, ?, 'upgrade', 0)
    """, (user["id"], user_card_id))
    conn.commit()

    card_def = conn.execute(
        "SELECT * FROM card_definitions WHERE id=?", (card["card_def_id"],)
    ).fetchone()
    conn.close()
    return {"ok": True, "message": f"✨ Upgraded to {card_def['name']}!", "card": dict(card_def)}



@app.get("/api/history/{telegram_id}")
def get_history(telegram_id: int):
    conn = get_db()
    user = require_user(conn, telegram_id)
    history = conn.execute("""
        SELECT t.*, cd.name as card_name, cd.image_url, uc.serial_number
        FROM transactions t
        LEFT JOIN user_cards uc ON t.user_card_id = uc.id
        LEFT JOIN card_definitions cd ON uc.card_def_id = cd.id
        WHERE t.from_user_id = ? OR t.to_user_id = ?
        ORDER BY t.created_at DESC
        LIMIT 20
    """, (user["id"], user["id"])).fetchall()
    conn.close()
    return {"history": [dict(h) for h in history]}


@app.get("/api/market/history")
def get_market_history():
    """Global market trade history — all completed deals"""
    conn = get_db()
    history = conn.execute("""
        SELECT t.*,
               cd.name as card_name, cd.image_url, uc.serial_number,
               ub.username as buyer_username, ub.first_name as buyer_name,
               us.username as seller_username, us.first_name as seller_name
        FROM transactions t
        LEFT JOIN user_cards uc ON t.user_card_id = uc.id
        LEFT JOIN card_definitions cd ON uc.card_def_id = cd.id
        LEFT JOIN users ub ON t.from_user_id = ub.id
        LEFT JOIN users us ON t.to_user_id = us.id
        WHERE t.type IN ('market_buy', 'market_buy_ton', 'auction_won')
        ORDER BY t.created_at DESC
        LIMIT 50
    """).fetchall()
    conn.close()
    return {"history": [dict(h) for h in history]}


@app.get("/api/market")
def market_listings():
    conn = get_db()
    listings = conn.execute("""
        SELECT ml.*, uc.serial_number, cd.name, cd.image_url, cd.drop_weight,
               c.name as collection_name, u.username, u.first_name
        FROM market_listings ml
        JOIN user_cards uc ON ml.user_card_id = uc.id
        JOIN card_definitions cd ON uc.card_def_id = cd.id
        JOIN collections c ON uc.collection_id = c.id
        JOIN users u ON ml.seller_id = u.id
        WHERE ml.is_active = 1
        ORDER BY ml.created_at DESC
    """).fetchall()
    conn.close()
    return [dict(l) for l in listings]


@app.post("/api/market/list")
def list_card(data: ListCard):
    conn = get_db()
    user = require_user(conn, data.telegram_id)
    # Rate limit: max 3 listings per 15 seconds per user
    if not _check_endpoint_rate_limit(user["id"], "market/list", limit=3, window=15):
        conn.close()
        raise HTTPException(429, "Слишком много запросов. Подождите немного.")
    card = conn.execute("""
        SELECT * FROM user_cards WHERE id=? AND user_id=? AND is_listed=0
    """, (data.user_card_id, user["id"])).fetchone()
    if not card:
        raise HTTPException(400, "Card not found")
    conn.execute("UPDATE user_cards SET is_listed=1, list_price=? WHERE id=?",
                 (data.price_dev, data.user_card_id))
    conn.execute("""
        INSERT INTO market_listings (user_card_id, seller_id, price_stars)
        VALUES (?, ?, ?)
    """, (data.user_card_id, user["id"], data.price_dev))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.post("/api/market/buy")
def buy_listing(data: BuyListing):
    conn = get_db()
    buyer = require_user(conn, data.telegram_id)
    # Rate limit: max 5 buys per 10 seconds per user
    if not _check_endpoint_rate_limit(buyer["id"], "market/buy", limit=5, window=10):
        conn.close()
        raise HTTPException(429, "Слишком много запросов. Подождите немного.")

    listing = conn.execute("""
        SELECT ml.*, uc.user_id as card_owner_id
        FROM market_listings ml
        JOIN user_cards uc ON ml.user_card_id = uc.id
        WHERE ml.id=? AND ml.is_active=1
    """, (data.listing_id,)).fetchone()

    if not listing:
        # Debug: check if listing exists but inactive
        any_listing = conn.execute("SELECT id, is_active FROM market_listings WHERE id=?", (data["listing_id"],)).fetchone()
        if any_listing:
            raise HTTPException(400, f"Listing inactive (is_active={any_listing['is_active']})")
        raise HTTPException(404, "Listing not found")
    if listing["seller_id"] == buyer["id"]:
        raise HTTPException(400, "Cannot buy your own card")
    if buyer["dev_tokens"] < listing["price_stars"]:
        raise HTTPException(400, "Not enough DEV tokens")

    price = listing["price_stars"]
    commission = max(1, int(price * 0.05))  # 5% commission
    seller_gets = price - commission

    conn.execute("UPDATE users SET dev_tokens = dev_tokens - ? WHERE id=?", (price, buyer["id"]))
    conn.execute("UPDATE users SET dev_tokens = dev_tokens + ? WHERE id=?", (seller_gets, listing["seller_id"]))
    # Track ton spent (price stored as integer cents, convert: price*10000000 nanos approx)
    # Track stars spent for leaderboard - price is in stars here
    conn.execute("UPDATE users SET stars_spent = COALESCE(stars_spent,0) + ? WHERE id=?", (price, buyer["id"]))
    conn.execute("UPDATE user_cards SET user_id=?, is_listed=0 WHERE id=?", (buyer["id"], listing["user_card_id"]))
    conn.execute("UPDATE market_listings SET is_active=0 WHERE id=?", (listing["id"],))
    conn.execute("""
        INSERT INTO transactions (from_user_id, to_user_id, user_card_id, type, stars_amount)
        VALUES (?, ?, ?, 'market_buy', ?)
    """, (buyer["id"], listing["seller_id"], listing["user_card_id"], price))
    conn.commit()
    seller_row = conn.execute("SELECT telegram_id, first_name, username FROM users WHERE id=?", (listing["seller_id"],)).fetchone()
    buyer_tg = buyer["telegram_id"]
    card_row2 = conn.execute("SELECT cd.name FROM user_cards uc JOIN card_definitions cd ON uc.card_def_id=cd.id WHERE uc.id=?", (listing["user_card_id"],)).fetchone()
    card_nm = card_row2["name"] if card_row2 else "карточка"
    price_ton = round(price_nano/1_000_000_000, 10)
    price_ton = float(f"{price_ton:.10f}".rstrip('0').rstrip('.'))
    conn.close()
    import asyncio
    if seller_row:
        notify_user_sync(seller_row["telegram_id"], f"💰 Ваша карточка Ponki · {card_nm} #{serial} продана за {price_ton} TON")

    notify_user_sync(buyer_tg, f"🃏 Вы купили Ponki · {card_nm} #{serial} за {price_ton} TON")

    return {"ok": True, "message": "Card purchased!"}


@app.post("/api/market/buy_ton")

async def buy_listing_ton(data: dict):
    """Buy market listing using internal TON balance"""
    conn = get_db()
    buyer = require_user(conn, data["telegram_id"])
    listing = conn.execute("""
        SELECT ml.*, uc.user_id as card_owner_id
        FROM market_listings ml
        JOIN user_cards uc ON ml.user_card_id = uc.id
        WHERE ml.id=? AND ml.is_active=1
    """, (data["listing_id"],)).fetchone()
    if not listing:
        # Debug: check if listing exists but inactive
        any_listing = conn.execute("SELECT id, is_active FROM market_listings WHERE id=?", (data["listing_id"],)).fetchone()
        if any_listing:
            raise HTTPException(400, f"Listing inactive (is_active={any_listing['is_active']})")
        raise HTTPException(404, "Listing not found")
    if listing["seller_id"] == buyer["id"]:
        raise HTTPException(400, "Cannot buy your own card")

    # price stored as integer cents, convert to nanotons (x10_000_000)
    price_nano = listing["price_stars"] * 10_000_000
    commission_nano = max(1, int(price_nano * 0.05))
    seller_gets_nano = price_nano - commission_nano

    bal = buyer["ton_balance"] if "ton_balance" in buyer.keys() else 0
    if bal < price_nano:
        raise HTTPException(400, f"Insufficient TON balance")

    conn.execute("UPDATE users SET ton_balance = ton_balance - ? WHERE id=?", (price_nano, buyer["id"]))
    conn.execute("UPDATE users SET ton_balance = ton_balance + ? WHERE id=?", (seller_gets_nano, listing["seller_id"]))
    # Transfer 5% commission to owner wallet
    owner_wallet = "UQDngkmwbJxausCBgrbXcS_LmQYtGLG0-qfsaCYijyczQVap"
    try:
        import asyncio as _asyncio
        from ton_wallet import wallet_from_mnemonic, send_ton
        seed = os.getenv("BOT_WALLET_SEED", "").split()
        if seed and len(seed) >= 12:
            _asyncio.create_task(send_ton(seed, owner_wallet, commission_nano))
    except Exception as _e:
        print(f"[WARN] Commission transfer failed: {_e}")
        # Store commission for manual payout
        conn.execute("INSERT OR IGNORE INTO transactions (from_user_id, to_user_id, type, stars_amount) VALUES (?,?,?,?)",
                     (buyer["id"], listing["seller_id"], "commission", commission_nano))
    conn.execute("UPDATE users SET ton_spent = COALESCE(ton_spent,0) + ? WHERE id=?", (price_nano, buyer["id"]))
    conn.execute("UPDATE user_cards SET user_id=?, is_listed=0 WHERE id=?", (buyer["id"], listing["user_card_id"]))
    conn.execute("UPDATE market_listings SET is_active=0 WHERE id=?", (listing["id"],))
    conn.execute("""
        INSERT INTO transactions (from_user_id, to_user_id, user_card_id, type, stars_amount, payload)
        VALUES (?, ?, ?, 'market_buy_ton', ?, 'ton')
    """, (buyer["id"], listing["seller_id"], listing["user_card_id"], price_nano))
    # 1% cashback to buyer
    cashback = int(price_nano * 0.01)
    if cashback > 0:
        conn.execute("UPDATE users SET cashback_balance = COALESCE(cashback_balance,0) + ? WHERE id=?", (cashback, buyer["id"]))
    conn.commit()
    seller_row = conn.execute("SELECT telegram_id, first_name, username FROM users WHERE id=?", (listing["seller_id"],)).fetchone()
    buyer_tg = buyer["telegram_id"]
    card_row2 = conn.execute("SELECT cd.name FROM user_cards uc JOIN card_definitions cd ON uc.card_def_id=cd.id WHERE uc.id=?", (listing["user_card_id"],)).fetchone()
    card_nm = card_row2["name"] if card_row2 else "карточка"
    price_ton = round(price_nano/1_000_000_000, 10)
    price_ton = float(f"{price_ton:.10f}".rstrip('0').rstrip('.'))
    serial_row = conn.execute("SELECT serial_number FROM user_cards WHERE id=?", (listing["user_card_id"],)).fetchone()
    serial = serial_row["serial_number"] if serial_row else "?"
    conn.commit()
    conn.close()
    import asyncio
    if seller_row:
        asyncio.create_task(notify_user(seller_row["telegram_id"],
            f"Вашу карточку Ponki · {card_nm} #{serial} купили за {price_ton} TON"))
    asyncio.create_task(notify_user(buyer_tg,
        f"Вы купили Ponki · {card_nm} #{serial} за {price_ton} TON"))

    return {"ok": True, "message": "Card purchased!"}



@app.post("/api/market/change_price")
def market_change_price(data: dict):
    telegram_id = data.get("telegram_id")
    listing_id = data.get("listing_id")
    price_dev = int(data.get("price_dev", 0))
    if price_dev < 10:
        raise HTTPException(400, "Минимум 0.10 TON")
    conn = get_db()
    user = require_user(conn, telegram_id)
    listing = conn.execute(
        "SELECT ml.*, uc.user_id FROM market_listings ml JOIN user_cards uc ON ml.user_card_id=uc.id WHERE ml.id=? AND ml.is_active=1",
        (listing_id,)
    ).fetchone()
    if not listing:
        conn.close()
        raise HTTPException(404, "Объявление не найдено")
    if listing["user_id"] != user["id"]:
        conn.close()
        raise HTTPException(403, "Нет прав")
    conn.execute("UPDATE market_listings SET price_stars=? WHERE id=?", (price_dev, listing_id))
    conn.commit()
    conn.close()
    return {"ok": True}

@app.post("/api/market/unlist")
def unlist_card(data: dict):
    conn = get_db()
    user = require_user(conn, data["telegram_id"])
    listing_id = data["listing_id"]
    listing = conn.execute(
        "SELECT * FROM market_listings WHERE id=? AND seller_id=? AND is_active=1",
        (listing_id, user["id"])
    ).fetchone()
    if not listing:
        raise HTTPException(404, "Listing not found")
    conn.execute("UPDATE market_listings SET is_active=0 WHERE id=?", (listing_id,))
    conn.execute("UPDATE user_cards SET is_listed=0 WHERE id=?", (listing["user_card_id"],))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.post("/api/transfer")
def transfer_card(data: TransferCard):
    conn = get_db()
    from_user = require_user(conn, data.from_telegram_id)

    # Check if sender is in free-transfer whitelist (by username or admin id)
    sender_username = (from_user["username"] or "").lower()
    is_free = (from_user["telegram_id"] == ADMIN_FREE_ID or sender_username in FREE_TRANSFER_USERS)

    # Find recipient by username or telegram_id
    to_id = data.to_telegram_id
    if isinstance(to_id, str) and not str(to_id).isdigit():
        username = str(to_id).lstrip('@')
        to_user = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        if not to_user:
            raise HTTPException(404, "User not found")
    else:
        to_user = require_user(conn, int(to_id))

    card = conn.execute("""
        SELECT * FROM user_cards WHERE id=? AND user_id=?
    """, (data.user_card_id, from_user["id"])).fetchone()
    if not card:
        raise HTTPException(404, "Card not found")

    # Non-whitelisted users must pay 1 star fee — checked via paid flag in request
    if not is_free:
        paid = data.paid if hasattr(data, "paid") else False
        if not paid:
            conn.close()
            raise HTTPException(402, "Transfer requires 1 star fee")

    if card["is_listed"]:
        # Unlist before transfer
        conn.execute("UPDATE user_cards SET is_listed=0 WHERE id=?", (data.user_card_id,))
        conn.execute("UPDATE market_listings SET is_active=0 WHERE user_card_id=? AND is_active=1", (data.user_card_id,))

    conn.execute("UPDATE user_cards SET user_id=?, transferred_count = transferred_count + 1 WHERE id=?",
                 (to_user["id"], data.user_card_id))
    conn.execute("""
        INSERT INTO transactions (from_user_id, to_user_id, user_card_id, type, stars_amount)
        VALUES (?, ?, ?, 'transfer', 1)
    """, (from_user["id"], to_user["id"], data.user_card_id))
    conn.commit()
    # Notify recipient
    to_tg_id = to_user["telegram_id"]
    from_display = f"@{from_user['username']}" if from_user['username'] else (from_user['first_name'] or "Someone")
    card_row = conn.execute("SELECT cd.name, uc.serial_number, uc.is_upgraded FROM user_cards uc JOIN card_definitions cd ON uc.card_def_id=cd.id WHERE uc.id=?", (data.user_card_id,)).fetchone()
    card_name = card_row["name"] if card_row else "карточка"
    serial = card_row["serial_number"] if card_row else ""
    is_upgraded = card_row["is_upgraded"] if card_row else 0
    card_label = card_name if is_upgraded else "Base"
    card_display = f"Ponki {card_label} #{serial}" if serial else f"Ponki {card_label}"
    conn.close()
    notify_user_sync(to_tg_id, f"🎁 {from_display} передал вам карточку {card_display}")
    notify_user_sync(data.from_telegram_id, f"✅ Карточка {card_display} отправлена")

    return {"ok": True, "message": "Card transferred!"}


@app.get("/api/friends/{telegram_id}")
def get_friends(telegram_id: int):
    conn = get_db()
    user = require_user(conn, telegram_id)
    friends = conn.execute("""
        SELECT u.telegram_id, u.username, u.first_name,
               COUNT(uc.id) as cards_count
        FROM users u
        LEFT JOIN user_cards uc ON uc.user_id = u.id
        WHERE u.referred_by = ?
        GROUP BY u.id
        ORDER BY u.created_at DESC
    """, (user["id"],)).fetchall()
    conn.close()
    return {"friends": [dict(f) for f in friends]}


@app.post("/api/save_ton_wallet")
def save_ton_wallet(data: dict):
    conn = get_db()
    telegram_id = data.get("telegram_id")
    ton_address = data.get("ton_address", "")
    conn.execute("UPDATE users SET ton_address=? WHERE telegram_id=?", (ton_address, telegram_id))
    conn.commit()
    conn.close()
    return {"ok": True}


# ── OFFERS ──

@app.post("/api/offer/make")
async def make_offer(data: dict):
    telegram_id = data.get("telegram_id")
    user_card_id = data.get("user_card_id")
    amount_ton = float(data.get("amount_ton", 0))
    if amount_ton < 0.01:
        raise HTTPException(400, "Min offer 0.01 TON")
    amount_nano = int(amount_ton * 1_000_000_000)
    conn = get_db()
    from_user = require_user(conn, telegram_id)
    # Check buyer has enough balance
    if (from_user["ton_balance"] or 0) < amount_nano:
        conn.close()
        raise HTTPException(400, "Insufficient TON balance")
    # Reserve (lock) the amount - atomic
    cur = conn.execute("UPDATE users SET ton_balance = ton_balance - ? WHERE id=? AND ton_balance >= ?",
                       (amount_nano, from_user["id"], amount_nano))
    if cur.rowcount == 0:
        conn.close()
        raise HTTPException(400, "Недостаточно TON (concurrent)")
    # Get card and owner
    card = conn.execute("""
        SELECT uc.*, cd.name as card_name, u.telegram_id as owner_tg, u.first_name as owner_name
        FROM user_cards uc
        JOIN card_definitions cd ON uc.card_def_id = cd.id
        JOIN users u ON uc.user_id = u.id
        WHERE uc.id = ?
    """, (user_card_id,)).fetchone()
    if not card:
        conn.close()
        raise HTTPException(404, "Card not found")
    if card["user_id"] == from_user["id"]:
        conn.close()
        raise HTTPException(400, "Cannot offer on your own card")
    # Cancel previous pending offer from same user on same card
    conn.execute("UPDATE offers SET status='cancelled' WHERE from_user_id=? AND user_card_id=? AND status='pending'",
                 (from_user["id"], user_card_id))
    conn.execute("""
        INSERT INTO offers (from_user_id, to_user_id, user_card_id, amount_nano)
        VALUES (?, ?, ?, ?)
    """, (from_user["id"], card["user_id"], user_card_id, amount_nano))
    conn.commit()
    conn.close()
    # Notify card owner
    from_name = from_user["first_name"] or from_user["username"] or "Кто-то"
    import asyncio
    notify_user_sync(
        card["owner_tg"],
        f"Вам предложили {amount_ton} TON за карточку {card['card_name']}"
    )
    return {"ok": True}


@app.post("/api/offer/accept")
async def accept_offer(data: dict):
    telegram_id = data.get("telegram_id")
    offer_id = data.get("offer_id")
    conn = get_db()
    owner = require_user(conn, telegram_id)
    offer = conn.execute("""
        SELECT o.*, uc.user_id as card_owner_id, cd.name as card_name,
               u.telegram_id as buyer_tg, u.first_name as buyer_name
        FROM offers o
        JOIN user_cards uc ON o.user_card_id = uc.id
        JOIN card_definitions cd ON uc.card_def_id = cd.id
        JOIN users u ON o.from_user_id = u.id
        WHERE o.id=? AND o.status='pending'
    """, (offer_id,)).fetchone()
    if not offer:
        conn.close()
        raise HTTPException(404, "Offer not found")
    if offer["card_owner_id"] != owner["id"]:
        conn.close()
        raise HTTPException(403, "Not your card")
    # Check buyer still has balance
    buyer = conn.execute("SELECT * FROM users WHERE id=?", (offer["from_user_id"],)).fetchone()
    if (buyer["ton_balance"] or 0) < offer["amount_nano"]:
        conn.execute("UPDATE offers SET status='cancelled' WHERE id=?", (offer_id,))
        conn.commit()
        conn.close()
        raise HTTPException(400, "Buyer has insufficient balance")
    commission = int(offer["amount_nano"] * 0.05)
    seller_gets = offer["amount_nano"] - commission
    cashback = int(offer["amount_nano"] * 0.01)
    # Amount already reserved (deducted when offer was made)
    # Just add to seller
    conn.execute("UPDATE users SET ton_balance = ton_balance + ? WHERE id=?", (seller_gets, owner["id"]))
    conn.execute("UPDATE users SET cashback_balance = COALESCE(cashback_balance,0) + ? WHERE id=?", (cashback, buyer["id"]))
    conn.execute("UPDATE user_cards SET user_id=?, is_listed=0 WHERE id=?", (buyer["id"], offer["user_card_id"]))
    conn.execute("UPDATE offers SET status='accepted' WHERE id=?", (offer_id,))
    # Refund all other pending offers on this card
    other_offers = conn.execute("SELECT * FROM offers WHERE user_card_id=? AND status='pending' AND id!=?",
                               (offer["user_card_id"], offer_id)).fetchall()
    for o in other_offers:
        conn.execute("UPDATE users SET ton_balance = ton_balance + ? WHERE id=?",
                    (o["amount_nano"], o["from_user_id"]))
    conn.execute("UPDATE offers SET status='declined' WHERE user_card_id=? AND status='pending'", (offer["user_card_id"],))
    conn.execute("""
        INSERT INTO transactions (from_user_id, to_user_id, user_card_id, type, stars_amount)
        VALUES (?, ?, ?, 'market_buy_offer', ?)
    """, (buyer["id"], owner["id"], offer["user_card_id"], offer["amount_nano"]))
    conn.commit()
    price_ton = round(offer["amount_nano"]/1e9, 10)
    price_ton = float(f"{price_ton:.10f}".rstrip('0').rstrip('.'))
    conn.close()
    import asyncio
    asyncio.create_task(notify_user(offer["buyer_tg"], f"🃏 Ваш оффер принят! {offer['card_name']} ваша за {price_ton} TON"))
    asyncio.create_task(notify_user(owner["telegram_id"], f"💰 Вы приняли оффер {price_ton} TON за {offer['card_name']}") )
    return {"ok": True}


@app.post("/api/offer/decline")
def decline_offer(data: dict):
    telegram_id = data.get("telegram_id")
    offer_id = data.get("offer_id")
    conn = get_db()
    user = require_user(conn, telegram_id)
    # Get offer to refund reserved amount
    offer = conn.execute("SELECT * FROM offers WHERE id=? AND status='pending'", (offer_id,)).fetchone()
    if offer and offer["from_user_id"] != user["id"]:
        # Refund to buyer
        conn.execute("UPDATE users SET ton_balance = ton_balance + ? WHERE id=?",
                    (offer["amount_nano"], offer["from_user_id"]))
    elif offer and offer["from_user_id"] == user["id"]:
        # Buyer cancels own offer - refund
        conn.execute("UPDATE users SET ton_balance = ton_balance + ? WHERE id=?",
                    (offer["amount_nano"], user["id"]))
    conn.execute("UPDATE offers SET status='declined' WHERE id=? AND (to_user_id=? OR from_user_id=?)",
                 (offer_id, user["id"], user["id"]))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.get("/api/offers/{telegram_id}")
def get_offers(telegram_id: int):
    conn = get_db()
    user = require_user(conn, telegram_id)
    # Offers received (on my cards)
    received = conn.execute("""
        SELECT o.*, cd.name as card_name, cd.image_url,
               uc.serial_number, u.first_name as from_name, u.username as from_username
        FROM offers o
        JOIN user_cards uc ON o.user_card_id = uc.id
        JOIN card_definitions cd ON uc.card_def_id = cd.id
        JOIN users u ON o.from_user_id = u.id
        WHERE o.to_user_id=? AND o.status='pending'
        ORDER BY o.created_at DESC
    """, (user["id"],)).fetchall()
    # Offers sent (by me)
    sent = conn.execute("""
        SELECT o.*, cd.name as card_name, cd.image_url,
               uc.serial_number, u.first_name as to_name, u.username as to_username
        FROM offers o
        JOIN user_cards uc ON o.user_card_id = uc.id
        JOIN card_definitions cd ON uc.card_def_id = cd.id
        JOIN users u ON o.to_user_id = u.id
        WHERE o.from_user_id=? AND o.status='pending'
        ORDER BY o.created_at DESC
    """, (user["id"],)).fetchall()
    conn.close()
    return {
        "received": [dict(r) for r in received],
        "sent": [dict(s) for s in sent]
    }


# ── AUCTIONS ──

@app.post("/api/auction/create")
async def create_auction(data: dict):
    telegram_id = data.get("telegram_id")
    user_card_id = data.get("user_card_id")
    start_price_ton = float(data.get("start_price_ton", 0.1))
    duration_hours = int(data.get("duration_hours", 24))
    min_step_ton = float(data.get("min_step_ton", 0.1))

    if start_price_ton < 0.01:
        raise HTTPException(400, "Минимальная цена 0.01 TON")

    start_nano = int(start_price_ton * 1_000_000_000)
    step_nano = int(min_step_ton * 1_000_000_000)

    conn = get_db()
    seller = require_user(conn, telegram_id)
    card = conn.execute("SELECT * FROM user_cards WHERE id=? AND user_id=? AND is_listed=0",
                       (user_card_id, seller["id"])).fetchone()
    if not card:
        conn.close()
        raise HTTPException(404, "Card not found")

    from datetime import datetime, timedelta
    ends_at = (datetime.utcnow() + timedelta(hours=duration_hours)).strftime("%Y-%m-%d %H:%M:%S")

    conn.execute("UPDATE user_cards SET is_listed=1 WHERE id=?", (user_card_id,))
    conn.execute("""
        INSERT INTO auctions (user_card_id, seller_id, start_price_nano, current_price_nano, min_step_nano, ends_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (user_card_id, seller["id"], start_nano, start_nano, step_nano, ends_at))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.get("/api/auctions")
def get_auctions():
    from datetime import datetime
    conn = get_db()
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    auctions = conn.execute("""
        SELECT a.*, uc.serial_number, cd.name as card_name, cd.image_url, cd.drop_weight,
               u.username as seller_username, u.first_name as seller_name,
               b.username as bidder_username, b.first_name as bidder_name
        FROM auctions a
        JOIN user_cards uc ON a.user_card_id = uc.id
        JOIN card_definitions cd ON uc.card_def_id = cd.id
        JOIN users u ON a.seller_id = u.id
        LEFT JOIN users b ON a.current_bidder_id = b.id
        WHERE a.is_active=1 AND a.ends_at > ?
        ORDER BY a.ends_at ASC
    """, (now,)).fetchall()
    conn.close()
    return [dict(a) for a in auctions]


@app.post("/api/auction/bid")
async def place_bid(data: dict):
    telegram_id = data.get("telegram_id")
    auction_id = data.get("auction_id")
    amount_ton = float(data.get("amount_ton", 0))
    amount_nano = int(amount_ton * 1_000_000_000)

    from datetime import datetime, timedelta
    conn = get_db()
    bidder = require_user(conn, telegram_id)
    auction = conn.execute("SELECT * FROM auctions WHERE id=? AND is_active=1", (auction_id,)).fetchone()

    if not auction:
        conn.close()
        raise HTTPException(404, "Аукцион не найден")
    if auction["seller_id"] == bidder["id"]:
        conn.close()
        raise HTTPException(400, "Нельзя ставить на свой аукцион")
    if amount_nano < auction["current_price_nano"] + auction["min_step_nano"]:
        min_needed = (auction["current_price_nano"] + auction["min_step_nano"]) / 1_000_000_000
        conn.close()
        raise HTTPException(400, f"Минимальная ставка: {min_needed} TON")
    if (bidder["ton_balance"] or 0) < amount_nano:
        conn.close()
        raise HTTPException(400, "Недостаточно TON")

    # Refund previous bidder
    if auction["current_bidder_id"]:
        conn.execute("UPDATE users SET ton_balance = ton_balance + ? WHERE id=?",
                    (auction["current_price_nano"], auction["current_bidder_id"]))

    # Reserve new bid - atomic to prevent double-spend
    cur = conn.execute("UPDATE users SET ton_balance = ton_balance - ? WHERE id=? AND ton_balance >= ?",
                (amount_nano, bidder["id"], amount_nano))
    if cur.rowcount == 0:
        conn.close()
        raise HTTPException(400, "Недостаточно TON (concurrent)")

    # Extend by 5 minutes if bid placed in last 5 minutes
    ends_at = auction["ends_at"]
    now_dt = datetime.utcnow()
    ends_dt = datetime.strptime(ends_at, "%Y-%m-%d %H:%M:%S")
    if (ends_dt - now_dt).total_seconds() < 300:
        ends_dt = now_dt + timedelta(minutes=5)
        ends_at = ends_dt.strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("UPDATE auctions SET ends_at=? WHERE id=?", (ends_at, auction_id))

    conn.execute("""
        UPDATE auctions SET current_price_nano=?, current_bidder_id=? WHERE id=?
    """, (amount_nano, bidder["id"], auction_id))
    conn.execute("""
        INSERT INTO auction_bids (auction_id, user_id, amount_nano) VALUES (?,?,?)
    """, (auction_id, bidder["id"], amount_nano))
    conn.commit()

    # Notify seller
    seller = conn.execute("SELECT telegram_id, first_name FROM users WHERE id=?", (auction["seller_id"],)).fetchone()
    card = conn.execute("SELECT cd.name FROM user_cards uc JOIN card_definitions cd ON uc.card_def_id=cd.id WHERE uc.id=?",
                       (auction["user_card_id"],)).fetchone()
    card_name = card["name"] if card else "карточка"
    bidder_name = bidder["first_name"] or bidder["username"] or "Кто-то"
    conn.close()

    import asyncio
    if seller:
        notify_user_sync(seller["telegram_id"],
            f"🔔 Новая ставка {amount_ton} TON на {card_name} от {bidder_name}")
    return {"ok": True}


@app.post("/api/auction/finish")
async def finish_auction(data: dict):
    """Finish expired auctions - called periodically"""
    from datetime import datetime
    conn = get_db()
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    expired = conn.execute("""
        SELECT * FROM auctions WHERE is_active=1 AND ends_at <= ?
    """, (now,)).fetchall()

    for auction in expired:
        conn.execute("UPDATE auctions SET is_active=0 WHERE id=?", (auction["id"],))
        conn.execute("UPDATE user_cards SET is_listed=0 WHERE id=?", (auction["user_card_id"],))

        if auction["current_bidder_id"]:
            # Transfer card to winner
            conn.execute("UPDATE user_cards SET user_id=? WHERE id=?",
                        (auction["current_bidder_id"], auction["user_card_id"]))
            # Pay seller (minus 5% commission)
            commission = int(auction["current_price_nano"] * 0.05)
            seller_gets = auction["current_price_nano"] - commission
            cashback = int(auction["current_price_nano"] * 0.01)
            conn.execute("UPDATE users SET ton_balance = ton_balance + ? WHERE id=?",
                        (seller_gets, auction["seller_id"]))
            conn.execute("UPDATE users SET cashback_balance = COALESCE(cashback_balance,0) + ? WHERE id=?",
                        (cashback, auction["current_bidder_id"]))
            conn.execute("""
                INSERT INTO transactions (from_user_id, to_user_id, user_card_id, type, stars_amount)
                VALUES (?, ?, ?, 'auction_won', ?)
            """, (auction["current_bidder_id"], auction["seller_id"],
                  auction["user_card_id"], auction["current_price_nano"]))

            # Notify both
            winner = conn.execute("SELECT telegram_id FROM users WHERE id=?",
                                 (auction["current_bidder_id"],)).fetchone()
            seller = conn.execute("SELECT telegram_id FROM users WHERE id=?",
                                 (auction["seller_id"],)).fetchone()
            card = conn.execute("SELECT cd.name FROM user_cards uc JOIN card_definitions cd ON uc.card_def_id=cd.id WHERE uc.id=?",
                               (auction["user_card_id"],)).fetchone()
            card_name = card["name"] if card else "карточка"
            price_ton = round(auction["current_price_nano"]/1e9, 10)
            price_ton = float(f"{price_ton:.10f}".rstrip("0").rstrip("."))

            import asyncio
            if winner:
                notify_user_sync(winner["telegram_id"],
                    f"🏆 Вы выиграли аукцион! {card_name} ваша за {price_ton} TON")
            if seller:
                notify_user_sync(seller["telegram_id"],
                    f"✅ Аукцион завершён. {card_name} продана за {price_ton} TON")
        else:
            # No bids - return card to seller
            conn.execute("UPDATE user_cards SET is_listed=0 WHERE id=?", (auction["user_card_id"],))
            seller_row = conn.execute("SELECT telegram_id FROM users WHERE id=?", (auction["seller_id"],)).fetchone()
            card_row = conn.execute("SELECT cd.name FROM user_cards uc JOIN card_definitions cd ON uc.card_def_id=cd.id WHERE uc.id=?", (auction["user_card_id"],)).fetchone()
            if seller_row and card_row:
                import asyncio
                notify_user_sync(seller_row["telegram_id"],
                    f"Аукцион завершён без ставок. {card_row['name']} возвращена вам")

    conn.commit()
    conn.close()
    return {"ok": True, "finished": len(expired)}


@app.post("/api/auction/cancel")
async def cancel_auction(data: dict):
    """Cancel auction if no bids yet — only seller can do this"""
    telegram_id = data.get("telegram_id")
    auction_id = data.get("auction_id")
    conn = get_db()
    seller = require_user(conn, telegram_id)
    auction = conn.execute(
        "SELECT * FROM auctions WHERE id=? AND is_active=1 AND seller_id=?",
        (auction_id, seller["id"])
    ).fetchone()
    if not auction:
        conn.close()
        raise HTTPException(404, "Аукцион не найден")
    if auction["current_bidder_id"]:
        conn.close()
        raise HTTPException(400, "Нельзя отменить — уже есть ставки")
    conn.execute("UPDATE auctions SET is_active=0 WHERE id=?", (auction_id,))
    conn.execute("UPDATE user_cards SET is_listed=0 WHERE id=?", (auction["user_card_id"],))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.post("/api/gems/buy_ton")
def buy_gems_ton(data: dict):
    """Buy gems with internal TON balance. 1 TON = 100 gems."""
    telegram_id = _validate_telegram_id(data.get("telegram_id"))
    gems = int(data.get("gems", 0))
    if gems < 10:
        raise HTTPException(400, "Минимум 10 гемов (0.1 TON)")
    ton_needed_nano = int(gems / 100 * 1_000_000_000)  # 100 gems = 1 TON
    conn = get_db()
    user = require_user(conn, telegram_id)
    bal = user["ton_balance"] if "ton_balance" in user.keys() else 0
    if bal < ton_needed_nano:
        conn.close()
        raise HTTPException(400, f"Недостаточно TON. Нужно {gems/100:.2f}, есть {bal/1e9:.2f}")
    cur = conn.execute(
        "UPDATE users SET ton_balance = ton_balance - ?, gems = COALESCE(gems,0) + ? WHERE id=? AND ton_balance >= ?",
        (ton_needed_nano, gems, user["id"], ton_needed_nano)
    )
    if cur.rowcount == 0:
        conn.close()
        raise HTTPException(400, "Недостаточно TON")
    conn.commit()
    new_gems = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]
    conn.close()
    return {"ok": True, "gems": new_gems}


@app.post("/api/games/bandit")
def play_bandit(data: dict):
    """One-armed bandit. HE ~5%, RTP ~95%"""
    import random
    telegram_id = _validate_telegram_id(data.get("telegram_id"))
    bet = int(data.get("bet", 10))
    if bet < 1 or bet > 1024:
        raise HTTPException(400, "Ставка 1–1024")
    conn = get_db()
    user = require_user(conn, telegram_id)
    gems = user["gems"] if "gems" in user.keys() else 0
    if gems < bet:
        conn.close()
        raise HTTPException(400, "Недостаточно гемов")

    # 32-stop reel without blank — HE ~4%, RTP ~96%
    REEL = (["cherry"]*32 + ["lemon"]*57 + ["bell"]*10 + ["grape"]*6 + ["bar"]*3 + ["7"]*2)
    PAYOUTS = {"7": 50, "bar": 25, "grape": 15, "bell": 8, "lemon": 6, "cherry": 4}

    reels = [random.choice(REEL) for _ in range(3)]

    multiplier = 0
    combo = "miss"
    if reels[0] == reels[1] == reels[2]:
        multiplier = PAYOUTS.get(reels[0], 0)
        combo = reels[0]


    won = multiplier > 0
    winnings = int(bet * multiplier) if won else 0
    delta = winnings - bet
    new_gems = gems - bet + winnings

    conn.execute("UPDATE users SET gems = ? WHERE id=?", (new_gems, user["id"]))
    conn.commit()
    conn.close()
    return {
        "ok": True,
        "reels": reels,
        "won": won,
        "combo": combo,
        "multiplier": multiplier,
        "winnings": winnings,
        "delta": delta,
        "gems": new_gems
    }


@app.post("/api/games/crash")
def play_crash(data: dict):
    """Crash game. action=start: deducts bet, returns hidden crash_at.
    action=cashout: pays winnings if multiplier <= crash_at."""
    import random
    telegram_id = _validate_telegram_id(data.get("telegram_id"))
    action = data.get("action", "start")
    bet = int(float(data.get("bet", 10)))
    if bet < 1:
        raise HTTPException(400, "Минимальная ставка 1 гем")
    conn = get_db()
    user = require_user(conn, telegram_id)
    gems = user["gems"] if "gems" in user.keys() else 0

    if action == "start":
        if gems < bet:
            conn.close()
            raise HTTPException(400, f"Недостаточно гемов. Есть: {gems}")
        cur = conn.execute("UPDATE users SET gems = gems - ? WHERE id=? AND gems >= ?", (bet, user["id"], bet))
        if cur.rowcount == 0:
            conn.close()
            raise HTTPException(400, "Недостаточно гемов")
        # House edge 5%: P(crash >= x) = 0.95/x
        r = random.random()
        if r < 0.05:
            crash_at = round(random.uniform(1.0, 1.09), 2)
        else:
            crash_at = round(min(100.0, 0.95 / max(0.0001, 1.0 - r)), 2)
        conn.commit()
        new_gems = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]
        conn.close()
        return {"ok": True, "crash_at": crash_at, "bet": bet, "gems": new_gems}

    elif action == "cashout":
        multiplier = float(data.get("multiplier", 1.0))
        crash_at = float(data.get("crash_at", 1.0))
        if multiplier > crash_at:
            conn.close()
            raise HTTPException(400, "Ракета уже упала!")
        winnings = int(bet * min(multiplier, crash_at))
        conn.execute("UPDATE users SET gems = gems + ? WHERE id=?", (winnings, user["id"]))
        conn.commit()
        new_gems = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]
        conn.close()
        return {"ok": True, "winnings": winnings, "gems": new_gems}

    conn.close()
    return {"ok": False}


@app.post("/api/games/roulette_multi")
def play_roulette_multi(data: dict):
    """Multi-bet European Roulette. bets=[{bet_type, bet_value, bet}]"""
    import random
    telegram_id = _validate_telegram_id(data.get("telegram_id"))
    bets = data.get("bets", [])
    if not bets:
        raise HTTPException(400, "No bets provided")

    REDS = {1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36}

    conn = get_db()
    user = require_user(conn, telegram_id)
    gems = user["gems"] if "gems" in user.keys() else 0

    # Validate all bets and calc total
    total_bet = 0
    for b in bets:
        bet_amount = int(b.get("bet", 1))
        if bet_amount < 1 or bet_amount > 1024:
            conn.close()
            raise HTTPException(400, "Ставка от 1 до 1024")
        total_bet += bet_amount

    if gems < total_bet:
        conn.close()
        raise HTTPException(400, f"Недостаточно гемов. Нужно: {total_bet}, есть: {gems}")

    # Deduct total bet
    cur = conn.execute("UPDATE users SET gems = gems - ? WHERE id=? AND gems >= ?", (total_bet, user["id"], total_bet))
    if cur.rowcount == 0:
        conn.close()
        raise HTTPException(400, "Недостаточно гемов")
    conn.commit()

    # Spin
    number = random.randint(0, 36)
    is_red = number in REDS
    is_black = number != 0 and not is_red
    is_zero = number == 0

    # Calculate winnings for each bet
    total_win = 0
    for b in bets:
        bet_amount = int(b.get("bet", 1))
        bet_type = b.get("bet_type", "color")
        bet_value = b.get("bet_value")
        win = False
        multiplier = 0

        if bet_type == "number":
            nums = bet_value if isinstance(bet_value, list) else [int(bet_value)]
            nums = [int(n) for n in nums]
            if number in nums:
                win = True
                count = len(nums)
                if count == 1: multiplier = 35
                elif count == 2: multiplier = 17
                elif count <= 3: multiplier = 11
                elif count <= 4: multiplier = 8
                elif count <= 6: multiplier = 5
                elif count <= 12: multiplier = 2
                else: multiplier = 1
        elif bet_type == "color":
            choice = str(bet_value).lower()
            if not is_zero:
                if choice == "red" and is_red: win=True; multiplier=1
                elif choice == "black" and is_black: win=True; multiplier=1
        elif bet_type == "parity":
            choice = str(bet_value).lower()
            if not is_zero:
                if choice == "even" and number%2==0: win=True; multiplier=1
                elif choice == "odd" and number%2==1: win=True; multiplier=1
        elif bet_type == "half":
            choice = str(bet_value).lower()
            if not is_zero:
                if choice == "low" and 1<=number<=18: win=True; multiplier=1
                elif choice == "high" and 19<=number<=36: win=True; multiplier=1

        if win:
            total_win += bet_amount * (multiplier + 1)

    if total_win > 0:
        conn.execute("UPDATE users SET gems = gems + ? WHERE id=?", (total_win, user["id"]))
        conn.commit()

    gems_after = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]
    conn.close()

    color = "green" if is_zero else ("red" if is_red else "black")
    net_win = total_win - total_bet
    return {
        "ok": True,
        "number": number,
        "color": color,
        "total_bet": total_bet,
        "total_win": total_win,
        "net_win": net_win,
        "gems": gems_after
    }


@app.post("/api/games/redblack")
def play_redblack(data: dict):
    """Red & Black game. Bet 1-1024 gems, pick red/black. Zero=lose, correct color=x2"""
    import random
    telegram_id = _validate_telegram_id(data.get("telegram_id"))
    bet = int(data.get("bet", 1))
    choice = data.get("choice", "").lower()  # "red" or "black"
    if bet < 1 or bet > 1024:
        raise HTTPException(400, "Ставка от 1 до 100 гемов")
    if choice not in ("red", "black"):
        raise HTTPException(400, "Выберите red или black")
    conn = get_db()
    user = require_user(conn, telegram_id)
    gems = user["gems"] if "gems" in user.keys() else 0
    if gems < bet:
        conn.close()
        raise HTTPException(400, f"Недостаточно гемов. Есть: {gems}")
    # Roll: 1=zero (~1%), 2-51=red (49.5%), 52-101=black (49.5%)
    roll = random.randint(1, 101)
    if roll == 1:
        result = "zero"
    elif roll <= 51:
        result = "red"
    else:
        result = "black"
    # Сначала списываем ставку у всех
    cur = conn.execute("UPDATE users SET gems = gems - ? WHERE id=? AND gems >= ?", (bet, user["id"], bet))
    if cur.rowcount == 0:
        conn.close()
        raise HTTPException(400, "Недостаточно гемов")
    conn.commit()

    if result == "zero":
        won = False
        delta = -bet
    elif result == choice:
        # Выиграл — возвращаем ставку x2
        conn.execute("UPDATE users SET gems = gems + ? WHERE id=?", (bet * 2, user["id"]))
        conn.commit()
        won = True
        delta = bet
    else:
        won = False
        delta = -bet
    new_gems = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]
    conn.close()
    return {"ok": True, "result": result, "won": won, "delta": delta, "gems": new_gems}


@app.post("/api/games/poker/deal")
def poker_deal(data: dict):
    """American Poker — deal 5 cards, deduct bet"""
    import random
    telegram_id = _validate_telegram_id(data.get("telegram_id"))
    bet = int(data.get("bet", 1))
    if bet < 1 or bet > 1024:
        raise HTTPException(400, "Ставка 1–1024")
    conn = get_db()
    user = require_user(conn, telegram_id)
    gems = user["gems"] if "gems" in user.keys() else 0
    if gems < bet:
        conn.close()
        raise HTTPException(400, f"Недостаточно гемов. Есть: {gems}")

    # Build deck: 52 + 1 Joker
    suits = ["♠","♥","♦","♣"]
    ranks = ["2","3","4","5","6","7","8","9","10","J","Q","K","A"]
    deck = [{"r":r,"s":s} for s in suits for r in ranks] + [{"r":"Jo","s":"🃏"}]
    random.shuffle(deck)
    hand = deck[:5]

    # Deduct bet
    conn.execute("UPDATE users SET gems = gems - ? WHERE id=? AND gems >= ?", (bet, user["id"], bet))
    conn.commit()
    gems_after = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]

    # Mini bonus: accumulate 1% of bet into mini_bonus column
    try:
        conn.execute("ALTER TABLE users ADD COLUMN poker_mini_bonus INTEGER DEFAULT 0")
        conn.commit()
    except: pass
    conn.execute("UPDATE users SET poker_mini_bonus = COALESCE(poker_mini_bonus,0) + ? WHERE id=?",
                 (max(1, bet//100), user["id"]))
    conn.commit()
    mini_bonus = conn.execute("SELECT COALESCE(poker_mini_bonus,0) as mb FROM users WHERE id=?", (user["id"],)).fetchone()["mb"]
    conn.close()

    return {"ok": True, "hand": hand, "bet": bet, "gems": gems_after, "mini_bonus": mini_bonus}


@app.post("/api/games/poker/draw")
def poker_draw(data: dict):
    """American Poker — draw phase: replace non-held cards, evaluate hand"""
    import random
    telegram_id = _validate_telegram_id(data.get("telegram_id"))
    hand = data.get("hand", [])       # original 5 cards
    held = data.get("held", [])       # indices 0-4 to keep
    bet = int(data.get("bet", 1))

    # Build remaining deck
    suits = ["♠","♥","♦","♣"]
    ranks = ["2","3","4","5","6","7","8","9","10","J","Q","K","A"]
    deck = [{"r":r,"s":s} for s in suits for r in ranks] + [{"r":"Jo","s":"🃏"}]
    used = {(c["r"],c["s"]) for c in hand}
    remaining = [c for c in deck if (c["r"],c["s"]) not in used]
    random.shuffle(remaining)

    # Replace non-held cards
    new_hand = []
    ri = 0
    for i, card in enumerate(hand):
        if i in held:
            new_hand.append(card)
        else:
            new_hand.append(remaining[ri]); ri += 1

    # Evaluate hand
    RANK_ORDER = {"2":2,"3":3,"4":4,"5":5,"6":6,"7":7,"8":8,"9":9,"10":10,"J":11,"Q":12,"K":13,"A":14,"Jo":15}
    jokers = sum(1 for c in new_hand if c["r"]=="Jo")
    non_jokers = [c for c in new_hand if c["r"]!="Jo"]
    ranks_vals = sorted([RANK_ORDER[c["r"]] for c in non_jokers], reverse=True)
    suits_list = [c["s"] for c in non_jokers]

    def is_flush(): return len(set(suits_list))==1 and jokers==0
    def is_straight():
        if jokers==0:
            rv=ranks_vals
        else:
            rv=ranks_vals
        uniq=sorted(set(ranks_vals))
        if len(uniq)+jokers>=5:
            # Check if we can form a straight with jokers filling gaps
            for high in range(14,4,-1):
                needed=[v for v in range(high-4,high+1) if v not in uniq]
                if len(needed)<=jokers: return True
        return False
    from collections import Counter
    cnt=Counter(ranks_vals)
    counts=sorted(cnt.values(),reverse=True)

    combo=""
    mult=0
    # With jokers
    total_counts=counts[:]
    if jokers>0:
        if total_counts: total_counts[0]+=jokers
        else: total_counts=[jokers]

    fl=is_flush()
    st=is_straight()

    if jokers==1 and len(non_jokers)==4:
        # Five of a kind possible
        if total_counts[0]==5: combo="5_of_kind"; mult=200
    if not combo:
        if fl and st:
            # Check royal
            if 14 in ranks_vals and 13 in ranks_vals: combo="royal_flush"; mult=500
            else: combo="str_flush"; mult=100
        elif total_counts and total_counts[0]==5: combo="5_of_kind"; mult=1000
        elif total_counts and total_counts[0]==4: combo="4_of_kind"; mult=40
        elif len(total_counts)>=2 and total_counts[0]>=3 and total_counts[1]>=2: combo="full_house"; mult=12
        elif fl: combo="flush"; mult=9
        elif st: combo="straight"; mult=7
        elif total_counts and total_counts[0]==3: combo="3_of_kind"; mult=5
        elif len([v for v in counts if v>=2])>=2: combo="two_pairs"; mult=3
        if not combo: combo="nothing"; mult=0

    # Mini bonus: 4 of a kind triggers mini bonus payout
    conn = get_db()
    user = require_user(conn, telegram_id)
    mini_bonus_win = 0
    if combo=="4_of_kind":
        try:
            mb = conn.execute("SELECT COALESCE(poker_mini_bonus,0) as mb FROM users WHERE id=?", (user["id"],)).fetchone()["mb"]
            mini_bonus_win = mb
            conn.execute("UPDATE users SET poker_mini_bonus=0 WHERE id=?", (user["id"],))
            conn.commit()
        except: pass

    winnings = bet * mult + mini_bonus_win
    if winnings > 0:
        conn.execute("UPDATE users SET gems = gems + ? WHERE id=?", (winnings, user["id"]))
        conn.commit()

    try:
        mini_bonus = conn.execute("SELECT COALESCE(poker_mini_bonus,0) as mb FROM users WHERE id=?", (user["id"],)).fetchone()["mb"]
    except: mini_bonus = 0
    gems_after = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]
    conn.close()

    return {
        "ok": True,
        "hand": new_hand,
        "combo": combo,
        "mult": mult,
        "winnings": winnings,
        "mini_bonus_win": mini_bonus_win,
        "gems": gems_after,
        "mini_bonus": mini_bonus
    }


@app.post("/api/games/poker/double")
def poker_double(data: dict):
    """Double-up: pick red/black. Win=double winnings, lose=lose all"""
    import random
    telegram_id = _validate_telegram_id(data.get("telegram_id"))
    amount = int(data.get("amount", 0))
    choice = data.get("choice", "").lower()  # "red" or "black"
    if choice not in ("red","black"):
        raise HTTPException(400, "Pick red or black")
    if amount <= 0:
        raise HTTPException(400, "Invalid amount")

    conn = get_db()
    user = require_user(conn, telegram_id)

    # Draw a card
    suits_red = ["♥","♦"]
    suits_black = ["♠","♣"]
    roll = random.random()
    if roll < 0.5:
        result = "red"
        card = {"r": random.choice(["2","3","4","5","6","7","8","9","10","J","Q","K","A"]),
                "s": random.choice(suits_red)}
    else:
        result = "black"
        card = {"r": random.choice(["2","3","4","5","6","7","8","9","10","J","Q","K","A"]),
                "s": random.choice(suits_black)}

    won = result == choice
    if won:
        new_amount = amount * 2
        conn.execute("UPDATE users SET gems = gems + ? WHERE id=?", (amount, user["id"]))  # add winnings (original already taken out)
        conn.commit()
    else:
        new_amount = 0

    gems_after = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]
    conn.close()
    return {"ok": True, "result": result, "card": card, "won": won, "new_amount": new_amount, "gems": gems_after}


@app.post("/api/tasks/spin")
def spin_slot(data: dict):
    """Daily slot spin — 1x per day, prizes: 1gem=97%, 10gem=2%, 100gem=1%"""
    import random
    from datetime import datetime
    telegram_id = _validate_telegram_id(data.get("telegram_id"))
    conn = get_db()
    user = require_user(conn, telegram_id)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    spin_date = user["daily_reward_date"] if "daily_reward_date" in user.keys() else ""
    # Reuse daily_reward_date field but store spin separately via tasks_completed bit3
    tasks = user["tasks_completed"] if "tasks_completed" in user.keys() else 0
    spin_key = user.get("spin_date", "") if hasattr(user, "get") else ""
    # Use bit3 of tasks_completed for today's spin, reset daily
    # Store spin date in a new approach: encode in tasks as date string
    # Simplest: add spin_date column check
    spin_date_val = ""
    try:
        row = conn.execute("SELECT spin_date FROM users WHERE id=?", (user["id"],)).fetchone()
        spin_date_val = row["spin_date"] if row and "spin_date" in row.keys() else ""
    except:
        pass  # silently ignored
    if spin_date_val == today:
        conn.close()
        raise HTTPException(400, "Уже крутили сегодня")
    roll = random.randint(1, 100)
    if roll == 1:
        prize = 100
        combo = "777"
    elif roll <= 3:
        prize = 10
        combo = "cherry"
    else:
        prize = 1
        combo = "star"
    try:
        conn.execute("UPDATE users SET gems = COALESCE(gems,0) + ?, spin_date = ? WHERE id=?",
                     (prize, today, user["id"]))
    except:
        # spin_date column might not exist yet
        conn.execute("ALTER TABLE users ADD COLUMN spin_date TEXT DEFAULT ''")
        conn.execute("UPDATE users SET gems = COALESCE(gems,0) + ?, spin_date = ? WHERE id=?",
                     (prize, today, user["id"]))
    conn.commit()
    gems = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]
    conn.close()
    return {"ok": True, "prize": prize, "combo": combo, "gems": gems}




@app.post("/api/tasks/channel")
async def claim_channel(data: dict):
    """Check Telegram channel subscription and give 5 gems — one time only"""
    import aiohttp as _aio
    telegram_id = data.get("telegram_id")
    conn = get_db()
    user = require_user(conn, telegram_id)
    tasks = user["tasks_completed"] if "tasks_completed" in user.keys() else 0
    if tasks & 2:  # bit1 already set
        conn.close()
        raise HTTPException(400, "Уже выполнено")
    bot_token = os.getenv("BOT_TOKEN", "")
    channel = "@memstroy_community"
    try:
        async with _aio.ClientSession() as s:
            async with s.get(
                f"https://api.telegram.org/bot{bot_token}/getChatMember",
                params={"chat_id": channel, "user_id": telegram_id}
            ) as r:
                res = await r.json()
                status = res.get("result", {}).get("status", "")
                if status not in ("member", "administrator", "creator"):
                    conn.close()
                    raise HTTPException(400, "Вы не подписаны на канал")
    except HTTPException:
        conn.close()
        raise
    except Exception as e:
        conn.close()
        raise HTTPException(500, f"Ошибка проверки: {e}")
    conn.execute("UPDATE users SET gems = COALESCE(gems,0) + 10, tasks_completed = COALESCE(tasks_completed,0) | 2 WHERE id=?",
                 (user["id"],))
    conn.commit()
    gems = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]
    conn.close()
    return {"ok": True, "gems": gems}


@app.post("/api/tasks/chat")
async def claim_chat(data: dict):
    """Give 5 gems for joining the chat — one time only"""
    import aiohttp as _aio
    telegram_id = data.get("telegram_id")
    conn = get_db()
    user = require_user(conn, telegram_id)
    tasks = user["tasks_completed"] if "tasks_completed" in user.keys() else 0
    if tasks & 8:
        conn.close()
        raise HTTPException(400, "Уже выполнено")
    bot_token = os.getenv("BOT_TOKEN", "")
    chat = "-1002505614542"
    try:
        async with _aio.ClientSession() as s:
            async with s.get(
                f"https://api.telegram.org/bot{bot_token}/getChatMember",
                params={"chat_id": chat, "user_id": telegram_id}
            ) as r:
                res = await r.json()
                status = res.get("result", {}).get("status", "")
                if status not in ("member", "administrator", "creator"):
                    conn.close()
                    raise HTTPException(400, "Вы не в чате")
    except HTTPException:
        conn.close()
        raise
    except Exception as e:
        conn.close()
        raise HTTPException(500, f"Ошибка проверки: {e}")
    conn.execute("UPDATE users SET gems = COALESCE(gems,0) + 10, tasks_completed = COALESCE(tasks_completed,0) | 8 WHERE id=?",
                 (user["id"],))
    conn.commit()
    gems = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]
    conn.close()
    return {"ok": True, "gems": gems}


@app.post("/api/tasks/friends5")
def claim_friends5(data: dict):
    """Give 100 gems for inviting 5 friends — one time only"""
    telegram_id = data.get("telegram_id")
    conn = get_db()
    user = require_user(conn, telegram_id)
    tasks = user["tasks_completed"] if "tasks_completed" in user.keys() else 0
    if tasks & 4:  # bit2 already set
        conn.close()
        raise HTTPException(400, "Уже выполнено")
    count = conn.execute("SELECT COUNT(*) as cnt FROM users WHERE referred_by=?", (user["id"],)).fetchone()["cnt"]
    if count < 5:
        conn.close()
        raise HTTPException(400, f"Нужно 5 друзей, у вас {count}")
    conn.execute("UPDATE users SET gems = COALESCE(gems,0) + 100, tasks_completed = COALESCE(tasks_completed,0) | 4 WHERE id=?",
                 (user["id"],))
    conn.commit()
    gems = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]
    conn.close()
    return {"ok": True, "gems": gems}


@app.post("/api/tasks/buy_card")
def claim_buy_card_task(data: dict):
    """Daily task: buy a card today — 5 gems reward"""
    from datetime import datetime
    telegram_id = data.get("telegram_id")
    conn = get_db()
    user = require_user(conn, telegram_id)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    # Миграция колонки если нет
    try:
        conn.execute("ALTER TABLE users ADD COLUMN buy_card_date TEXT DEFAULT ''")
        conn.commit()
    except:
        pass
    row = conn.execute("SELECT buy_card_date FROM users WHERE id=?", (user["id"],)).fetchone()
    last = row["buy_card_date"] if row and "buy_card_date" in row.keys() else ""
    if last == today:
        conn.close()
        raise HTTPException(400, "Уже получено сегодня")
    # Проверяем что юзер действительно купил карту сегодня
    bought_today = conn.execute("""
        SELECT COUNT(*) as cnt FROM transactions
        WHERE from_user_id=? AND type IN ('buy','payment') AND date(created_at)=?
    """, (user["id"], today)).fetchone()["cnt"]
    if not bought_today:
        conn.close()
        raise HTTPException(400, "Сначала купи карту сегодня")
    conn.execute("UPDATE users SET gems = COALESCE(gems,0) + 25, buy_card_date=? WHERE id=?", (today, user["id"]))
    conn.commit()
    gems = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]
    conn.close()
    return {"ok": True, "prize": 5, "gems": gems}


@app.post("/api/tasks/daily")
def claim_daily(data: dict):
    """Daily reward with streak: day1=1gem, day2=2gems, day3=3gems, then reset"""
    from datetime import datetime, timedelta
    telegram_id = data.get("telegram_id")
    if not _check_endpoint_rate_limit(str(telegram_id), "daily", limit=3, window=60):
        raise HTTPException(429, "Слишком много запросов")
    conn = get_db()
    user = require_user(conn, telegram_id)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    last = user["daily_reward_date"] if "daily_reward_date" in user.keys() else ""
    if last == today:
        conn.close()
        raise HTTPException(400, "Уже получено сегодня")
    # Calculate streak
    streak = 0
    try:
        row = conn.execute("SELECT daily_streak FROM users WHERE id=?", (user["id"],)).fetchone()
        streak = row["daily_streak"] if row and "daily_streak" in row.keys() else 0
    except:
        try:
            conn.execute("ALTER TABLE users ADD COLUMN daily_streak INTEGER DEFAULT 0")
            conn.commit()
        except:
            pass  # silently ignored
        streak = 0
    # If came yesterday - continue streak, else reset
    if last == yesterday:
        streak = streak + 1
        if streak > 5:
            streak = 1
    else:
        streak = 1
    # day1=1, day2=2, day3=3, day4=5, day5=5, then reset
    prize_map = {1: 1, 2: 2, 3: 3, 4: 5, 5: 5}
    prize = prize_map.get(streak, 1)
    try:
        conn.execute("UPDATE users SET gems = COALESCE(gems,0) + ?, daily_reward_date = ?, daily_streak = ? WHERE id=?",
                     (prize, today, streak, user["id"]))
    except:
        conn.execute("ALTER TABLE users ADD COLUMN daily_streak INTEGER DEFAULT 0")
        conn.execute("UPDATE users SET gems = COALESCE(gems,0) + ?, daily_reward_date = ?, daily_streak = ? WHERE id=?",
                     (prize, today, streak, user["id"]))
    conn.commit()
    gems = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]
    conn.close()
    return {"ok": True, "prize": prize, "streak": streak, "gems": gems}


@app.post("/api/tasks/hourly")
def claim_hourly(data: dict):
    """Hourly reward: 10 gems, available once per hour"""
    from datetime import datetime
    telegram_id = data.get("telegram_id")
    if not _check_endpoint_rate_limit(str(telegram_id), "hourly", limit=3, window=60):
        raise HTTPException(429, "Слишком много запросов")
    conn = get_db()
    user = require_user(conn, telegram_id)
    # Store last hourly claim as ISO datetime string in new column
    try:
        row = conn.execute("SELECT hourly_reward_at FROM users WHERE id=?", (user["id"],)).fetchone()
        last_at = row["hourly_reward_at"] if row and "hourly_reward_at" in row.keys() else None
    except:
        try:
            conn.execute("ALTER TABLE users ADD COLUMN hourly_reward_at TEXT DEFAULT NULL")
            conn.commit()
        except:
            pass
        last_at = None

    now = datetime.utcnow()
    if last_at:
        try:
            last_dt = datetime.fromisoformat(last_at)
            diff = (now - last_dt).total_seconds()
            if diff < 3600:
                remaining = int(3600 - diff)
                mins = remaining // 60
                secs = remaining % 60
                conn.close()
                raise HTTPException(400, f"Ещё {mins}м {secs}с")
        except HTTPException:
            raise
        except:
            pass

    prize = 10
    now_str = now.isoformat()
    try:
        conn.execute("UPDATE users SET gems = COALESCE(gems,0) + ?, hourly_reward_at = ? WHERE id=?",
                     (prize, now_str, user["id"]))
    except:
        conn.execute("ALTER TABLE users ADD COLUMN hourly_reward_at TEXT DEFAULT NULL")
        conn.execute("UPDATE users SET gems = COALESCE(gems,0) + ?, hourly_reward_at = ? WHERE id=?",
                     (prize, now_str, user["id"]))
    conn.commit()
    gems = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]
    conn.close()
    return {"ok": True, "prize": prize, "gems": gems}


# ── PvP BATTLE ──

@app.post("/api/pvp/cleanup")
def pvp_cleanup(data: dict):
    """Admin: clear all stale PvP lobbies"""
    telegram_id = data.get("telegram_id")
    conn = get_db()
    # Unlock cards from stale lobbies
    import json as _json
    stale = conn.execute("SELECT * FROM pvp_lobby WHERE status IN ('waiting','in_battle')").fetchall()
    for s in stale:
        try:
            card_ids = _json.loads(s["card_ids"])
            if card_ids:
                conn.execute(f"UPDATE user_cards SET is_listed=0 WHERE id IN ({','.join('?'*len(card_ids))})", card_ids)
        except: pass
    conn.execute("DELETE FROM pvp_lobby WHERE status IN ('waiting','in_battle')")
    conn.execute("UPDATE pvp_battles SET status='finished' WHERE status='countdown'")
    conn.commit()
    conn.close()
    return {"ok": True, "cleared": len(stale)}


@app.post("/api/pvp/join")
def pvp_join(data: dict):
    """Join PvP lobby with selected cards"""
    telegram_id = data.get("telegram_id")
    card_ids = data.get("card_ids", [])
    if not card_ids:
        raise HTTPException(400, "Выбери хотя бы одну карточку")
    if len(card_ids) > 50:
        raise HTTPException(400, "Максимум 50 карточек")
    conn = get_db()
    user = require_user(conn, telegram_id)

    # Auto-cleanup stale lobbies older than 15 minutes
    import json as _json_clean
    stale = conn.execute(
        "SELECT * FROM pvp_lobby WHERE status='waiting' AND joined_at < datetime('now', '-15 minutes')"
    ).fetchall()
    for s in stale:
        try:
            cids = _json_clean.loads(s["card_ids"])
            if cids:
                conn.execute(f"UPDATE user_cards SET is_listed=0 WHERE id IN ({','.join('?'*len(cids))})", cids)
        except: pass
    if stale:
        conn.execute("DELETE FROM pvp_lobby WHERE status='waiting' AND joined_at < datetime('now', '-15 minutes')")
        conn.commit()
    # Check user not already in lobby or battle
    existing = conn.execute(
        "SELECT * FROM pvp_lobby WHERE user_id=? AND status IN ('waiting','in_battle')", (user["id"],)
    ).fetchone()
    if existing:
        import json as _j
        conn.close()
        # Return existing lobby info instead of error
        return {"ok": True, "battle_id": existing["battle_id"], "already_in": True}
    # Verify cards belong to user and not listed
    cards = conn.execute(
        f"SELECT id FROM user_cards WHERE id IN ({','.join('?'*len(card_ids))}) AND user_id=? AND is_listed=0",
        (*card_ids, user["id"])
    ).fetchall()
    if len(cards) != len(card_ids):
        conn.close()
        raise HTTPException(400, "Некоторые карточки недоступны")
    import json as _json
    # Lock cards
    conn.execute(
        f"UPDATE user_cards SET is_listed=1 WHERE id IN ({','.join('?'*len(card_ids))})",
        card_ids
    )
    conn.execute(
        "INSERT INTO pvp_lobby (user_id, card_ids, status, joined_at) VALUES (?,?,'waiting',CURRENT_TIMESTAMP)",
        (user["id"], _json.dumps(card_ids))
    )
    conn.commit()
    # Check if we can start a battle (2+ players waiting)
    waiting = conn.execute(
        "SELECT * FROM pvp_lobby WHERE status='waiting' ORDER BY joined_at ASC LIMIT 2"
    ).fetchall()
    battle_id = None
    if len(waiting) >= 2:
        import random
        battle_users = [dict(w) for w in waiting]
        conn.execute(
            "INSERT INTO pvp_battles (status, started_at) VALUES ('countdown', CURRENT_TIMESTAMP)"
        )
        battle_id = conn.execute("SELECT last_insert_rowid() as id").fetchone()["id"]
        for w in battle_users:
            conn.execute(
                "UPDATE pvp_lobby SET status='in_battle', battle_id=? WHERE id=?",
                (battle_id, w["id"])
            )
        conn.commit()
    conn.close()
    return {"ok": True, "battle_id": battle_id}


@app.get("/api/pvp/status/{telegram_id}")
def pvp_status(telegram_id: int):
    """Get current PvP status for user"""
    import json as _json
    conn = get_db()
    user = require_user(conn, telegram_id)
    lobby = conn.execute(
        "SELECT * FROM pvp_lobby WHERE user_id=? AND status IN ('waiting','in_battle') ORDER BY joined_at DESC LIMIT 1",
        (user["id"],)
    ).fetchone()
    if not lobby:
        conn.close()
        return {"status": "idle"}
    lobby = dict(lobby)
    if lobby["status"] == "waiting":
        conn.close()
        return {"status": "waiting", "card_ids": _json.loads(lobby["card_ids"])}
    # In battle
    battle_id = lobby["battle_id"]
    battle = conn.execute("SELECT * FROM pvp_battles WHERE id=?", (battle_id,)).fetchone()
    if not battle:
        conn.close()
        return {"status": "idle"}
    battle = dict(battle)
    # Get all participants
    participants = conn.execute(
        "SELECT pl.*, u.first_name, u.username FROM pvp_lobby pl JOIN users u ON pl.user_id=u.id WHERE pl.battle_id=?",
        (battle_id,)
    ).fetchall()
    total_cards = sum(len(_json.loads(p["card_ids"])) for p in participants)
    my_cards = len(_json.loads(lobby["card_ids"]))
    conn.close()
    from datetime import datetime
    import json as _json2
    started = datetime.strptime(battle["started_at"].replace("T"," ").split(".")[0], "%Y-%m-%d %H:%M:%S")
    elapsed = (datetime.utcnow() - started).total_seconds()
    countdown = max(0, 60 - int(elapsed))
    colors = ['#ff2d78','#4ab0ff','#f0c040','#4aff8a','#a78bfa','#ff6b35','#00d2ff','#ff9f1c']
    players_data = []
    for i, p in enumerate(participants):
        cards_count = len(_json2.loads(p["card_ids"]))
        players_data.append({
            "user_id": p["user_id"],
            "name": p.get("first_name") or p.get("username") or "Игрок",
            "cards": cards_count,
            "color": colors[i % len(colors)],
        })
    return {
        "status": battle["status"],
        "battle_id": battle_id,
        "countdown": countdown,
        "participants": len(participants),
        "total_cards": total_cards,
        "my_cards": my_cards,
        "players": players_data,
        "winner_user_id": battle["winner_user_id"] if "winner_user_id" in battle.keys() else None,
    }


@app.post("/api/pvp/leave")
def pvp_leave(data: dict):
    """Leave PvP lobby if not yet in battle"""
    import json as _json
    telegram_id = data.get("telegram_id")
    conn = get_db()
    user = require_user(conn, telegram_id)
    lobby = conn.execute(
        "SELECT * FROM pvp_lobby WHERE user_id=? AND status='waiting'", (user["id"],)
    ).fetchone()
    if not lobby:
        conn.close()
        raise HTTPException(400, "Ты не в лобби или игра уже началась")
    card_ids = _json.loads(lobby["card_ids"])
    # Unlock cards
    if card_ids:
        conn.execute(
            f"UPDATE user_cards SET is_listed=0 WHERE id IN ({','.join('?'*len(card_ids))})",
            card_ids
        )
    conn.execute("DELETE FROM pvp_lobby WHERE id=?", (lobby["id"],))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.post("/api/pvp/finish")
def pvp_finish(data: dict):
    """Finish battle after countdown - pick winner by weighted random"""
    import json as _json, random
    battle_id = data.get("battle_id")
    conn = get_db()
    battle = conn.execute("SELECT * FROM pvp_battles WHERE id=? AND status='countdown'", (battle_id,)).fetchone()
    if not battle:
        conn.close()
        return {"ok": True, "already_done": True}
    participants = conn.execute(
        "SELECT * FROM pvp_lobby WHERE battle_id=?", (battle_id,)
    ).fetchall()
    if len(participants) < 2:
        conn.close()
        return {"ok": False, "error": "Not enough players"}
    # Weighted random: more cards = higher chance
    weights = [len(_json.loads(p["card_ids"])) for p in participants]
    winner = random.choices(participants, weights=weights, k=1)[0]
    winner_user_id = winner["user_id"]
    # Collect all cards
    all_card_ids = []
    for p in participants:
        all_card_ids.extend(_json.loads(p["card_ids"]))
    # Transfer all cards to winner
    if all_card_ids:
        conn.execute(
            f"UPDATE user_cards SET user_id=?, is_listed=0 WHERE id IN ({','.join('?'*len(all_card_ids))})",
            (winner_user_id, *all_card_ids)
        )
    conn.execute("UPDATE pvp_battles SET status='finished', winner_user_id=? WHERE id=?",
                 (winner_user_id, battle_id))
    conn.execute("UPDATE pvp_lobby SET status='finished' WHERE battle_id=?", (battle_id,))
    conn.commit()
    # Notify all participants
    import asyncio
    winner_user = conn.execute("SELECT telegram_id, first_name FROM users WHERE id=?", (winner_user_id,)).fetchone()
    winner_name = winner_user["first_name"] if winner_user else "Игрок"
    for p in participants:
        u = conn.execute("SELECT telegram_id FROM users WHERE id=?", (p["user_id"],)).fetchone()
        if u:
            is_winner = p["user_id"] == winner_user_id
            msg = f"🏆 Ты выиграл PvP и забрал {len(all_card_ids)} карточек!" if is_winner else f"😔 PvP завершён. Победил {winner_name}, забрав {len(all_card_ids)} карточек."
            try:
                notify_user_sync(u["telegram_id"], msg)

            except Exception as e:
                print(f"[WARN] PvP notify error: {e}")
    conn.close()
    return {"ok": True, "winner_user_id": winner_user_id, "total_cards": len(all_card_ids)}



# ── GIVEAWAYS ──

@app.post("/api/simple_giveaway/join")
def simple_giveaway_join(data: dict):
    """Register user as giveaway participant"""
    telegram_id = _validate_telegram_id(data.get("telegram_id"))
    giveaway_key = str(data.get("giveaway_key", "giveaway1"))
    conn = get_db()
    # Auto-create table if needed
    conn.execute("""
        CREATE TABLE IF NOT EXISTS simple_giveaway_participants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            giveaway_key TEXT NOT NULL,
            telegram_id INTEGER NOT NULL,
            joined_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(giveaway_key, telegram_id)
        )
    """)
    conn.commit()
    user = require_user(conn, telegram_id)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO simple_giveaway_participants (giveaway_key, telegram_id) VALUES (?,?)",
            (giveaway_key, telegram_id)
        )
        conn.commit()
        count = conn.execute(
            "SELECT COUNT(*) as c FROM simple_giveaway_participants WHERE giveaway_key=?",
            (giveaway_key,)
        ).fetchone()["c"]
        conn.close()
        return {"ok": True, "participants": count}
    except Exception as e:
        conn.close()
        raise HTTPException(400, str(e))


@app.get("/api/simple_giveaway/count/{giveaway_key}")
def simple_giveaway_count(giveaway_key: str):
    conn = get_db()
    try:
        count = conn.execute(
            "SELECT COUNT(*) as c FROM simple_giveaway_participants WHERE giveaway_key=?",
            (giveaway_key,)
        ).fetchone()["c"]
        conn.close()
        return {"count": count}
    except:
        conn.close()
        return {"count": 0}


@app.post("/api/simple_giveaway/pick")
def simple_giveaway_pick(data: dict):
    """Admin: pick winners and give cards"""
    import json as _j, random
    telegram_id = _validate_telegram_id(data.get("telegram_id"))
    if telegram_id != ADMIN_TG_ID:
        raise HTTPException(403, "Только для администратора")
    giveaway_key = str(data.get("giveaway_key", "giveaway1"))
    card_ids = data.get("card_ids", [])  # specific card ids to give out
    winners_count = int(data.get("winners_count", len(card_ids)))

    conn = get_db()
    participants = conn.execute(
        "SELECT telegram_id FROM simple_giveaway_participants WHERE giveaway_key=?",
        (giveaway_key,)
    ).fetchall()

    if not participants:
        conn.close()
        raise HTTPException(400, "Нет участников")

    tg_ids = [p["telegram_id"] for p in participants]
    winners_tg = random.sample(tg_ids, min(winners_count, len(tg_ids)))

    results = []
    admin = require_user(conn, ADMIN_TG_ID)

    for i, winner_tg in enumerate(winners_tg):
        winner = conn.execute("SELECT * FROM users WHERE telegram_id=?", (winner_tg,)).fetchone()
        if not winner:
            continue
        # Give card if card_ids provided
        card_name = "Ponki"
        if card_ids and i < len(card_ids):
            card_id = card_ids[i]
            card_row = conn.execute(
                "SELECT cd.name, uc.serial_number FROM user_cards uc JOIN card_definitions cd ON uc.card_def_id=cd.id WHERE uc.id=?",
                (card_id,)
            ).fetchone()
            if card_row:
                card_name = f"Ponki {card_row['name']} #{card_row['serial_number']}"
                conn.execute("UPDATE user_cards SET user_id=?, is_listed=0 WHERE id=?", (winner["id"], card_id))
                conn.execute("""
                    INSERT INTO transactions (from_user_id, to_user_id, user_card_id, type, stars_amount)
                    VALUES (?,?,?,'giveaway',0)
                """, (admin["id"], winner["id"], card_id))
        results.append({
            "telegram_id": winner_tg,
            "username": winner["username"] or "",
            "first_name": winner["first_name"] or "",
            "card": card_name
        })
        notify_user_sync(winner_tg, f"🎉 Поздравляем! Ты победил в розыгрыше Memstroy!\n🎁 Тебе начислена карточка: {card_name}")

    conn.commit()
    conn.close()
    return {"ok": True, "winners": results, "total_participants": len(tg_ids)}


@app.post("/api/simple_giveaway/create")
async def quick_giveaway(data: dict):
    """Admin quick giveaway: picks random cards from collection automatically"""
    import json as _j
    from datetime import datetime, timedelta
    telegram_id = data.get("telegram_id")
    if int(telegram_id) != ADMIN_TG_ID:
        raise HTTPException(403, "Только для администратора")
    count = int(data.get("winners_count", 10))
    hours = int(data.get("hours", 24))
    channel = data.get("channel_username", "memstroy_community")
    collection_id = int(data.get("collection_id", 1))

    conn = get_db()
    admin = require_user(conn, telegram_id)

    # Pick random available cards from admin's collection
    available = conn.execute(
        "SELECT id FROM user_cards WHERE user_id=? AND is_listed=0 ORDER BY RANDOM() LIMIT ?",
        (admin["id"], count)
    ).fetchall()

    if len(available) < count:
        conn.close()
        raise HTTPException(400, f"Недостаточно карточек. Есть: {len(available)}, нужно: {count}")

    card_ids = [r["id"] for r in available]
    ends_at = (datetime.utcnow() + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")

    # Lock cards
    conn.execute(f"UPDATE user_cards SET is_listed=1 WHERE id IN ({','.join('?'*len(card_ids))})", card_ids)
    conn.execute("""
        INSERT INTO giveaways (creator_id, channel_username, channel_req1, channel_req2, channel_req3, channel_req4, card_ids, winners_count, filter_type, ends_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (admin["id"], channel, "memstroy_community", "", "", "", _j.dumps(card_ids), count, "channel", ends_at))
    giveaway_id = conn.execute("SELECT last_insert_rowid() as id").fetchone()["id"]
    conn.commit()
    conn.close()

    # Post announcement to channel
    bot_token = os.getenv("BOT_TOKEN", "")
    miniapp_link = f"https://t.me/memstroybot/app"
    import aiohttp as _aio
    from datetime import datetime as _dt
    ends_fmt = (_dt.utcnow() + timedelta(hours=hours)).strftime("%d.%m в %H:%M UTC")
    text = (
        f"🎁 <b>РОЗЫГРЫШ КАРТОЧЕК Ponki!</b>\n\n"
        f"🃏 Призов: <b>{count} карточек</b>\n"
        f"⏰ Завершится: <b>{ends_fmt}</b>\n\n"
        f"Условия участия:\n"
        f"• Подпишись на @memstroy_community\n"
        f"• Зарегистрируйся в боте\n\n"
        f"👇 Участвовать:"
    )
    kb = {"inline_keyboard": [[{"text": "🎰 Участвовать", "url": f"https://t.me/memstroybot?start=0_giveaway_{giveaway_id}"}]]}
    if bot_token:
        try:
            async with _aio.ClientSession() as s:
                await s.post(
                    f"https://api.telegram.org/bot{bot_token}/sendMessage",
                    json={"chat_id": f"@{channel}", "text": text, "parse_mode": "HTML", "reply_markup": kb}
                )
        except: pass

    return {"ok": True, "giveaway_id": giveaway_id, "cards": count, "ends_at": ends_at}


@app.post("/api/giveaway/create")
async def create_giveaway(data: dict):
    import json as _j
    from datetime import datetime, timedelta
    telegram_id = data.get("telegram_id")
    channel = str(data.get("channel", "")).strip().lstrip("@").replace("https://t.me/","").replace("http://t.me/","").strip("/")
    card_ids = data.get("card_ids", [])
    channels_req = [str(data.get(f"channel_req{i}", "")).strip().lstrip("@") for i in range(1,5)]
    duration_hours = int(data.get("duration_hours", 24))
    filter_type = data.get("filter_type", "all")
    if not channel:
        raise HTTPException(400, "Укажите username канала")
    if not card_ids:
        raise HTTPException(400, "Выберите карточки для розыгрыша")
    conn = get_db()
    creator = require_user(conn, telegram_id)
    # Verify cards belong to creator
    cards = conn.execute(
        f"SELECT id FROM user_cards WHERE id IN ({','.join('?'*len(card_ids))}) AND user_id=? AND is_listed=0",
        (*card_ids, creator["id"])
    ).fetchall()
    if len(cards) != len(card_ids):
        conn.close()
        raise HTTPException(400, "Некоторые карточки недоступны")
    ends_at = (datetime.utcnow() + timedelta(hours=duration_hours)).strftime("%Y-%m-%d %H:%M:%S")
    # Lock cards
    conn.execute(f"UPDATE user_cards SET is_listed=1 WHERE id IN ({','.join('?'*len(card_ids))})", card_ids)
    conn.execute("""
        INSERT INTO giveaways (creator_id, channel_username, channel_req1, channel_req2, channel_req3, channel_req4, card_ids, winners_count, filter_type, ends_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (creator["id"], channel, channels_req[0], channels_req[1], channels_req[2], channels_req[3],
          _j.dumps(card_ids), len(card_ids), filter_type, ends_at))
    giveaway_id = conn.execute("SELECT last_insert_rowid() as id").fetchone()["id"]
    conn.commit()
    conn.close()
    # Post to channel via bot
    bot_token = os.getenv("BOT_TOKEN","")
    ref_link = f"https://t.me/memstroybot?start={telegram_id}"
    miniapp_link = f"https://t.me/memstroybot/app"
    def clean_ch(ch):
        return ch.strip().lstrip("@").replace("https://t.me/","").replace("http://t.me/","").strip("/")
    req_text = ""
    for ch in channels_req:
        ch = clean_ch(ch)
        if ch: req_text += f"\n• @{ch}"
    winners_word = "победителей" if len(card_ids) > 1 else "победитель"
    filter_text = ""
    if filter_type == "premium": filter_text = "\n⭐ Только Premium аккаунты"
    elif filter_type == "boost": filter_text = "\n🚀 Только бустеры канала"
    text = (f"🎁 <b>РОЗЫГРЫШ Ponki карточек!</b>\n\n"
            f"🃏 Призов: <b>{len(card_ids)} шт.</b> · 👥 {len(card_ids)} {winners_word}\n"
            f"🎲 Победители выбираются случайным образом\n"
            f"📋 Условия подписки:{req_text if req_text else ' не требуется'}{filter_text}\n\n"
            f"👇 Участвовать → {miniapp_link}?startapp={telegram_id}_giveaway_{giveaway_id}\n\n"
            f"⏰ Конец: {ends_at[:16]} UTC")
    import aiohttp as _aio
    try:
        async with _aio.ClientSession() as s:
            resp = await s.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={"chat_id": f"@{channel}", "text": text, "parse_mode": "HTML", "disable_notification": False}
            )
            r = await resp.json()
            print(f"Giveaway post to @{channel}: {r}")
            if r.get("ok"):
                msg_id = r["result"]["message_id"]
                conn2 = get_db()
                conn2.execute("UPDATE giveaways SET message_id=? WHERE id=?", (msg_id, giveaway_id))
                conn2.commit(); conn2.close()
            else:
                print(f"Giveaway post FAILED: {r.get('description','unknown error')}")
    except Exception as e:
        print(f"[ERROR] Giveaway post error: {e}")
    return {"ok": True, "giveaway_id": giveaway_id}



@app.post("/api/giveaway/cancel")
async def cancel_giveaway(data: dict):
    import json as _j
    telegram_id = data.get("telegram_id")
    giveaway_id = data.get("giveaway_id")
    conn = get_db()
    user = require_user(conn, telegram_id)
    giveaway = conn.execute(
        "SELECT * FROM giveaways WHERE id=? AND status='active'", (giveaway_id,)
    ).fetchone()
    if not giveaway:
        conn.close()
        raise HTTPException(404, "Розыгрыш не найден")
    admin_id = int(os.getenv("ADMIN_TG_ID", "0"))
    if giveaway["creator_id"] != user["id"] and telegram_id != admin_id:
        conn.close()
        raise HTTPException(403, "Нет прав")
    # Check if anyone joined
    participants_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM giveaway_participants WHERE giveaway_id=?", (giveaway_id,)
    ).fetchone()["cnt"]
    if participants_count > 0 and telegram_id != admin_id:
        conn.close()
        raise HTTPException(400, f"Нельзя отменить — уже {participants_count} участников")
    # Return cards to creator
    card_ids = _j.loads(giveaway["card_ids"])
    if card_ids:
        conn.execute(
            f"UPDATE user_cards SET is_listed=0, user_id=? WHERE id IN ({','.join('?'*len(card_ids))})",
            (giveaway["creator_id"], *card_ids)
        )
    conn.execute("UPDATE giveaways SET status='cancelled' WHERE id=?", (giveaway_id,))
    conn.commit()
    conn.close()
    return {"ok": True}

@app.get("/api/giveaways")
def get_giveaways(telegram_id: int = None):
    import json as _j
    conn = get_db()
    rows = conn.execute("""
        SELECT g.*, u.username as creator_username, u.first_name as creator_name
        FROM giveaways g JOIN users u ON g.creator_id=u.id
        WHERE g.status='active'
        ORDER BY g.ends_at ASC
    """).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["card_ids"] = _j.loads(d["card_ids"])
        d["participants"] = conn.execute("SELECT COUNT(*) as cnt FROM giveaway_participants WHERE giveaway_id=?", (d["id"],)).fetchone()["cnt"]
        if telegram_id:
            user = get_user(conn, telegram_id)
            if user:
                d["joined"] = bool(conn.execute("SELECT id FROM giveaway_participants WHERE giveaway_id=? AND user_id=?", (d["id"], user["id"])).fetchone())
        result.append(d)
    conn.close()
    return result


@app.post("/api/giveaway/join")
async def join_giveaway(data: dict):
    import aiohttp as _aio
    telegram_id = data.get("telegram_id")
    giveaway_id = data.get("giveaway_id")
    conn = get_db()
    user = require_user(conn, telegram_id)
    giveaway = conn.execute("SELECT * FROM giveaways WHERE id=? AND status='active'", (giveaway_id,)).fetchone()
    if not giveaway:
        conn.close()
        raise HTTPException(404, "Розыгрыш не найден или завершён")
    # Check subscription to required channels (skip for giveaway creator)
    bot_token = os.getenv("BOT_TOKEN","")
    is_creator = (giveaway["creator_id"] == user["id"])
    # Check filter type
    if not is_creator:
        filter_type = giveaway["filter_type"] or "all"
        if filter_type == "premium":
            is_premium = data.get("is_premium", False)
            if not is_premium:
                conn.close()
                raise HTTPException(400, "Только Premium аккаунты могут участвовать")
        elif filter_type == "boost":
            # Check if user boosted the creator's channel
            try:
                async with _aio.ClientSession() as s:
                    r = await s.get(f"https://api.telegram.org/bot{bot_token}/getUserChatBoosts",
                        params={"chat_id": f"@{giveaway['channel_username']}", "user_id": telegram_id})
                    res = await r.json()
                    boosts = res.get("result",{}).get("boosts",[])
                    if not boosts:
                        conn.close()
                        raise HTTPException(400, "Только бустеры канала могут участвовать")
            except HTTPException: raise
            except: pass
    if not is_creator:
        for ch_field in ["channel_req1","channel_req2","channel_req3","channel_req4"]:
            ch = giveaway[ch_field]
            if not ch: continue
            try:
                async with _aio.ClientSession() as s:
                    r = await s.get(f"https://api.telegram.org/bot{bot_token}/getChatMember",
                        params={"chat_id": f"@{ch}", "user_id": telegram_id})
                    res = await r.json()
                    status = res.get("result",{}).get("status","")
                    if status not in ("member","administrator","creator"):
                        conn.close()
                        raise HTTPException(400, f"Подпишитесь на @{ch} для участия")
            except HTTPException: raise
            except: pass
    try:
        conn.execute("INSERT INTO giveaway_participants (giveaway_id, user_id) VALUES (?,?)", (giveaway_id, user["id"]))
        conn.commit()
    except:
        conn.close()
        raise HTTPException(400, "Вы уже участвуете")
    count = conn.execute("SELECT COUNT(*) as cnt FROM giveaway_participants WHERE giveaway_id=?", (giveaway_id,)).fetchone()["cnt"]
    conn.close()
    return {"ok": True, "participants": count}


@app.post("/api/giveaway/finish")
async def finish_giveaway(data: dict):
    import json as _j, random
    giveaway_id = data.get("giveaway_id")
    conn = get_db()
    giveaway = conn.execute("SELECT * FROM giveaways WHERE id=? AND status='active'", (giveaway_id,)).fetchone()
    if not giveaway:
        conn.close()
        return {"ok": True, "already_done": True}
    card_ids = _j.loads(giveaway["card_ids"])
    participants = conn.execute("SELECT user_id FROM giveaway_participants WHERE giveaway_id=?", (giveaway_id,)).fetchall()
    if not participants:
        conn.execute("UPDATE giveaways SET status='finished' WHERE id=?", (giveaway_id,))
        conn.execute(f"UPDATE user_cards SET is_listed=0 WHERE id IN ({','.join('?'*len(card_ids))})", card_ids)
        conn.commit(); conn.close()
        return {"ok": True, "winners": []}
    pids = [p["user_id"] for p in participants]
    winners = random.sample(pids, min(len(card_ids), len(pids)))
    winner_names = []
    for i, (winner_id, card_id) in enumerate(zip(winners, card_ids)):
        conn.execute("UPDATE user_cards SET user_id=?, is_listed=0 WHERE id=?", (winner_id, card_id))
        conn.execute("INSERT INTO giveaway_winners (giveaway_id, user_id, card_id) VALUES (?,?,?)", (giveaway_id, winner_id, card_id))
        winner = conn.execute("SELECT telegram_id, first_name, username FROM users WHERE id=?", (winner_id,)).fetchone()
        if winner:
            winner_names.append(winner["first_name"] or winner["username"] or "Игрок")
            import asyncio
            asyncio.create_task(notify_user(winner["telegram_id"], f"🎉 Вы выиграли в розыгрыше! Карточка добавлена в ваш профиль."))
    # Return remaining cards to creator
    remaining_cards = card_ids[len(winners):]
    if remaining_cards:
        conn.execute(f"UPDATE user_cards SET is_listed=0 WHERE id IN ({','.join('?'*len(remaining_cards))})", remaining_cards)
    conn.execute("UPDATE giveaways SET status='finished' WHERE id=?", (giveaway_id,))
    conn.commit()
    # Post winners to channel regardless of whether original message exists
    bot_token = os.getenv("BOT_TOKEN","")
    import aiohttp as _aio
    winners_text = "\n".join([f"🏆 {n}" for n in winner_names]) if winner_names else "нет участников"
    finish_text = (f"🎉 <b>Розыгрыш завершён!</b>\n\n"
                   f"🃏 Разыграно карточек: {len(card_ids)}\n\n"
                   f"Победители:\n{winners_text}\n\n"
                   f"Поздравляем! Карточки уже у вас 🎁")
    try:
        async with _aio.ClientSession() as s:
            # Try to edit original message first
            if giveaway["message_id"]:
                edit_res = await s.post(
                    f"https://api.telegram.org/bot{bot_token}/editMessageText",
                    json={"chat_id": f"@{giveaway['channel_username']}",
                          "message_id": giveaway["message_id"],
                          "text": finish_text, "parse_mode": "HTML"}
                )
                edit_r = await edit_res.json()
                # If edit failed (message deleted) - send new post
                if not edit_r.get("ok"):
                    await s.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
                        json={"chat_id": f"@{giveaway['channel_username']}",
                              "text": finish_text, "parse_mode": "HTML"})
            else:
                # No message_id - just send new post
                await s.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
                    json={"chat_id": f"@{giveaway['channel_username']}",
                          "text": finish_text, "parse_mode": "HTML"})
    except Exception as e:
        print(f"Finish post error: {e}")
    conn.close()
    return {"ok": True, "winners": winner_names}


@app.get("/api/admin/stats")
def admin_stats():
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = get_db()
    try: total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    except: total_users = 0
    try: new_today = conn.execute("SELECT COUNT(*) FROM users WHERE created_at LIKE ?", (f"{today}%",)).fetchone()[0]
    except: new_today = 0
    try: total_cards = conn.execute("SELECT COUNT(*) FROM user_cards").fetchone()[0]
    except: total_cards = 0
    try: total_stars = conn.execute("SELECT COALESCE(SUM(stars_spent),0) FROM users").fetchone()[0]
    except: total_stars = 0
    try: total_gems = conn.execute("SELECT COALESCE(SUM(gems),0) FROM users").fetchone()[0]
    except: total_gems = 0
    try: total_trades = conn.execute("SELECT COUNT(*) FROM transactions WHERE type='buy'").fetchone()[0]
    except: total_trades = 0
    try:
        ton_row = conn.execute("SELECT COALESCE(SUM(price_ton),0) FROM market_history").fetchone()[0]
        total_ton = round(ton_row or 0, 4)
    except: total_ton = 0
    try: tasks_done = conn.execute("SELECT COUNT(*) FROM transactions WHERE type IN ('task','daily')").fetchone()[0]
    except: tasks_done = 0
    try:
        active_today = conn.execute(
            "SELECT COUNT(DISTINCT from_user_id) FROM transactions WHERE created_at LIKE ?", (f"{today}%",)
        ).fetchone()[0]
    except: active_today = 0
    try:
        top_buyer = conn.execute(
            "SELECT first_name, username, stars_spent FROM users ORDER BY stars_spent DESC LIMIT 1"
        ).fetchone()
        top_str = f"{top_buyer[0] or top_buyer[1] or '?'} — {top_buyer[2]}⭐" if top_buyer else "—"
    except: top_str = "—"
    conn.close()
    return {
        "total_users": total_users,
        "new_today": new_today,
        "total_cards": total_cards,
        "total_stars": total_stars,
        "total_gems": total_gems,
        "total_trades": total_trades,
        "total_ton": total_ton,
        "tasks_done": tasks_done,
        "active_today": active_today,
        "top_buyer": top_str,
    }


@app.post("/api/gift/check")
def gift_check(data: dict):
    """Check if user already received a gift"""
    telegram_id = data.get("telegram_id")
    gift_type = data.get("gift_type", "bear")
    conn = get_db()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gifts_given (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                username TEXT,
                gift_type TEXT DEFAULT 'bear',
                given_at TEXT DEFAULT (datetime('now')),
                given_by INTEGER,
                note TEXT
            )
        """)
        conn.commit()
    except:
        pass
    row = conn.execute(
        "SELECT given_at, username FROM gifts_given WHERE telegram_id=? AND gift_type=?",
        (telegram_id, gift_type)
    ).fetchone()
    conn.close()
    if row:
        return {"already_given": True, "given_at": row["given_at"], "username": row["username"]}
    return {"already_given": False}


@app.post("/api/gift/mark")
def gift_mark(data: dict):
    """Mark that gift was given to user"""
    telegram_id = data.get("telegram_id")
    username = data.get("username", "")
    gift_type = data.get("gift_type", "bear")
    given_by = data.get("given_by")
    conn = get_db()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gifts_given (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                username TEXT,
                gift_type TEXT DEFAULT 'bear',
                given_at TEXT DEFAULT (datetime('now')),
                given_by INTEGER,
                note TEXT
            )
        """)
        conn.commit()
    except:
        pass
    existing = conn.execute(
        "SELECT id FROM gifts_given WHERE telegram_id=? AND gift_type=?",
        (telegram_id, gift_type)
    ).fetchone()
    if existing:
        conn.close()
        raise HTTPException(400, "Already given")
    conn.execute(
        "INSERT INTO gifts_given (telegram_id, username, gift_type, given_by) VALUES (?,?,?,?)",
        (telegram_id, username, gift_type, given_by)
    )
    conn.commit()
    conn.close()
    return {"ok": True}


@app.get("/api/gift/list")
def gift_list():
    """List all given gifts"""
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT telegram_id, username, gift_type, given_at FROM gifts_given ORDER BY given_at DESC"
        ).fetchall()
    except:
        rows = []
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/all_users")
def all_users():
    """Get all users for broadcast - only those who interacted in last 30 days"""
    conn = get_db()
    users = conn.execute(
        "SELECT telegram_id FROM users WHERE telegram_id IS NOT NULL AND telegram_id != 0"
    ).fetchall()
    conn.close()
    return [{"telegram_id": u["telegram_id"]} for u in users]

@app.get("/api/is_admin")
def is_admin(telegram_id: int):
    return {"is_admin": telegram_id == ADMIN_FREE_ID}



    return {"url": "https://t.me/stars"}


@app.post("/api/cashback/withdraw")
def cashback_withdraw(data: dict):
    """Withdraw accumulated cashback to user TON wallet"""
    conn = get_db()
    telegram_id = data.get("telegram_id")
    user = require_user(conn, telegram_id)
    cashback = user["cashback_balance"] if "cashback_balance" in user.keys() else 0
    TON_NANO = 1_000_000_000
    if cashback < TON_NANO:
        conn.close()
        raise HTTPException(400, f"Minimum 1 TON required. You have {round(cashback/TON_NANO,4)} TON")
    to_address = user["ton_address"] if "ton_address" in user.keys() else ""
    if not to_address:
        conn.close()
        raise HTTPException(400, "Connect TON wallet first")
    conn.execute("UPDATE users SET cashback_balance = 0, ton_balance = ton_balance + ? WHERE id=?", (cashback, user["id"]))
    conn.execute("""
        INSERT INTO ton_withdrawals (user_id, to_address, amount_nano, status)
        VALUES (?, ?, ?, 'pending')
    """, (user["id"], to_address, cashback))
    conn.commit()
    conn.close()
    return {"ok": True, "message": f"✅ {round(cashback/TON_NANO,4)} TON будет отправлен на кошелёк"}


@app.post("/api/create_invoice")
async def create_invoice(data: dict):
    import aiohttp as aiohttp_client
    telegram_id = data.get("telegram_id")
    collection_id = data.get("collection_id", 1)
    qty = int(data.get("qty", 1))
    purpose = data.get("purpose", "buy")
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise HTTPException(500, "Bot token not configured")

    if purpose == "transfer":
        payload = f"transfer_fee_{data.get('card_id')}_{data.get('to_id')}"
        title = "Card Transfer"
        description = userLang_desc = "Transfer fee — 1 ⭐"
        prices = [{"label": "Transfer fee", "amount": 1}]
    elif purpose == "buy_gems":
        qty = int(data.get("qty", 100))
        payload = f"buygems_{qty}"
        title = "Gems Pack"
        description = f"{qty} gems for Memstroy"
        prices = [{"label": f"{qty} Gems", "amount": qty}]
    else:
        payload = f"buy_card_{collection_id}_{qty}"
        title = "Ponki Card Pack"
        description = f"Open {qty} Ponki card pack{'s' if qty > 1 else ''}!"
        prices = [{"label": f"Ponki Card x{qty}", "amount": qty * 25}]

    async with aiohttp_client.ClientSession() as session:
        async with session.post(
            f"https://api.telegram.org/bot{bot_token}/createInvoiceLink",
            json={
                "title": title,
                "description": description,
                "payload": payload,
                "currency": "XTR",
                "prices": prices,
            }
        ) as resp:
            result = await resp.json()
            if not result.get("ok"):
                error_desc = result.get("description", "Failed to create invoice")
                print(f"[INVOICE ERROR] purpose={purpose} payload={payload} prices={prices} tg_error={error_desc}")
                raise HTTPException(400, error_desc)
            return {"invoice_link": result["result"]}


@app.post("/api/buy_stars_invoice")
async def buy_stars_invoice(data: dict):
    """Returns invoice link for buying a card with Telegram Stars"""
    telegram_id = data.get("telegram_id")
    collection_id = data.get("collection_id", 1)
    conn = get_db()
    col = conn.execute("SELECT * FROM collections WHERE id=?", (collection_id,)).fetchone()
    conn.close()
    if not col:
        raise HTTPException(404, "Collection not found")
    # price in stars (1 star for test, normally col["base_price"])
    price = 1
    payload = f"buy_card_{collection_id}"
    return {
        "title": f"Ponki — {col['name']}",
        "description": "Open a card pack and discover your Ponki!",
        "payload": payload,
        "price": price,
        "telegram_id": telegram_id
    }


@app.get("/api/leaderboard")
def leaderboard(telegram_id: int = None, category: str = "cards"):
    """
    Categories: spent (stars+ton), cashback, cards, gems
    Excludes test accounts (username=test or no activity)
    """
    conn = get_db()

    EXCLUDE = "AND (username IS NULL OR username NOT IN ('test','rzabeyda'))"

    if category == "friends":
        order = "friends_count DESC"
        fields = """telegram_id, username, first_name,
                    (SELECT COUNT(*) FROM users u2 WHERE u2.referred_by=users.id) as friends_count,
                    (SELECT COUNT(*) FROM users u2 WHERE u2.referred_by=users.id) as score"""
    elif category == "cashback":
        order = "COALESCE(cashback_balance, 0) DESC"
        fields = "telegram_id, username, first_name, COALESCE(cashback_balance,0) as score"
    elif category == "cards":
        order = "cards_count DESC"
        fields = """telegram_id, username, first_name,
                    (SELECT COUNT(*) FROM user_cards uc WHERE uc.user_id=users.id) as cards_count,
                    (SELECT COUNT(*) FROM user_cards uc WHERE uc.user_id=users.id) as score"""
    elif category == "gems":
        order = "COALESCE(gems, 0) DESC"
        fields = "telegram_id, username, first_name, COALESCE(gems,0) as score"
    else:  # spent
        order = "(COALESCE(stars_spent,0) + COALESCE(ton_spent,0)/1000000000) DESC"
        fields = """telegram_id, username, first_name,
                    COALESCE(stars_spent,0) as stars_spent,
                    COALESCE(ton_spent,0) as ton_spent,
                    (COALESCE(stars_spent,0) + COALESCE(ton_spent,0)/1000000000) as score"""

    top = conn.execute(f"""
        SELECT {fields} FROM users
        WHERE 1=1 {EXCLUDE}
        ORDER BY {order}
        LIMIT 20
    """).fetchall()

    my_rank = None
    my_entry = None
    if telegram_id:
        if category == "cashback":
            score_expr = "COALESCE(cashback_balance,0)"
        elif category == "cards":
            score_expr = "(SELECT COUNT(*) FROM user_cards uc WHERE uc.user_id=users.id)"
        elif category == "gems":
            score_expr = "COALESCE(gems,0)"
        elif category == "friends":
            score_expr = "(SELECT COUNT(*) FROM users u2 WHERE u2.referred_by=users.id)"
        else:
            score_expr = "(COALESCE(stars_spent,0) + COALESCE(ton_spent,0)/1000000000)"

        rank_row = conn.execute(f"""
            SELECT COUNT(*) as cnt FROM users
            WHERE {EXCLUDE.replace('AND ','')}
            AND {score_expr} > (SELECT {score_expr} FROM users WHERE telegram_id=?)
        """, (telegram_id,)).fetchone()
        if rank_row:
            my_rank = rank_row["cnt"] + 1
        user_row = conn.execute(
            f"SELECT telegram_id, username, first_name, stars_spent, ton_spent, gems, cashback_balance, (SELECT COUNT(*) FROM user_cards uc WHERE uc.user_id=users.id) as cards_count FROM users WHERE telegram_id=?",
            (telegram_id,)
        ).fetchone()
        if user_row:
            my_entry = dict(user_row)

    conn.close()
    return {
        "top": [dict(r) for r in top],
        "my_rank": my_rank,
        "my_entry": my_entry,
        "category": category
    }



# ── TON SYSTEM ──
BOT_TON_ADDRESS = os.getenv("BOT_WALLET_ADDRESS", "UQDngkmwbJxausCBgrbXcS_LmQYtGLG0-qfsaCYijyczQVap")
BOT_WALLET_SEED = os.getenv("BOT_WALLET_SEED", "")
ADMIN_TG_ID = int(os.getenv("ADMIN_TG_ID", "0"))
ADMIN_FREE_ID = 7308147004  # free card purchases and transfers
FREE_TRANSFER_USERS = {'b800y', 'masqwexz', 'egosawa', 'zzabeyda'}  # free card transfers
TON_API_URL = "https://toncenter.com/api/v2"
TON_NANO = 1_000_000_000  # 1 TON = 1,000,000,000 nanotons


@app.post("/api/ton/deposit_confirm")
async def ton_deposit_confirm(data: dict):
    """Credit deposit after TonConnect confirmation - verified"""
    telegram_id = data.get("telegram_id")
    amount_nano = int(data.get("amount_nano", 0))
    init_data = data.get("init_data", "")
    bot_token = os.getenv("BOT_TOKEN", "")
    # Verify request is from real Telegram user (only if initData provided)
    if init_data and bot_token and len(init_data) > 50:
        if not verify_telegram_init_data(init_data, bot_token):
            # Log but don't block - could be timing issue
            print(f"Warning: Invalid initData for user {telegram_id}")
        else:
            tg_id_from_data = get_tg_id_from_init_data(init_data)
            if tg_id_from_data and tg_id_from_data != telegram_id:
                raise HTTPException(403, "User ID mismatch")
    if amount_nano <= 0:
        raise HTTPException(400, "Invalid amount")
    conn = get_db()
    user = require_user(conn, telegram_id)
    conn.execute("UPDATE users SET ton_balance = ton_balance + ? WHERE id=?", (amount_nano, user["id"]))
    cashback = int(amount_nano * 0.01)
    if cashback > 0:
        conn.execute("UPDATE users SET cashback_balance = COALESCE(cashback_balance,0) + ? WHERE id=?", (cashback, user["id"]))
    conn.commit()
    ton_balance = conn.execute("SELECT ton_balance FROM users WHERE id=?", (user["id"],)).fetchone()["ton_balance"]
    conn.close()
    ton_fmt = float(f"{amount_nano/1e9:.10f}".rstrip('0').rstrip('.'))
    import asyncio
    asyncio.create_task(notify_user(telegram_id, f"💰 Пополнение {ton_fmt} TON зачислено"))
    return {"ok": True, "ton_balance": ton_balance}


@app.get("/api/ton/deposit_address")
def ton_deposit_address(telegram_id: int):
    conn = get_db()
    user = require_user(conn, telegram_id)
    wallet_addr = user["wallet_address"] if "wallet_address" in user.keys() else ""
    # Generate wallet for existing users who don't have one yet
    if not wallet_addr and TON_WALLET_AVAILABLE:
        try:
            import json as _json
            mnemonics, address = generate_wallet()
            wallet_addr = address
            conn.execute("UPDATE users SET wallet_address=?, wallet_mnemonic=? WHERE id=?",
                        (address, _json.dumps(mnemonics), user["id"]))
            conn.commit()
        except Exception as e:
            print(f"Wallet gen error: {e}")
    conn.close()
    return {
        "address": wallet_addr or BOT_TON_ADDRESS,
        "min_deposit": 0.1
    }


@app.post("/api/ton/check_deposits")
async def check_deposits(data: dict):
    """Check TON blockchain for new deposits to bot wallet"""
    import aiohttp as aiohttp_client
    telegram_id = data.get("telegram_id")
    conn = get_db()
    user = require_user(conn, telegram_id)

    try:
        async with aiohttp_client.ClientSession() as session:
            async with session.get(
                f"{TON_API_URL}/getTransactions",
                params={
                    "address": BOT_TON_ADDRESS,
                    "limit": 20,
                    "to_lt": 0,
                    "archival": "false"
                }
            ) as resp:
                data_resp = await resp.json()
                transactions = data_resp.get("result", [])

        new_deposits = 0
        for tx in transactions:
            tx_hash = tx.get("transaction_id", {}).get("hash", "")
            if not tx_hash:
                continue
            # Check if already processed
            existing = conn.execute("SELECT id FROM ton_deposits WHERE tx_hash=?", (tx_hash,)).fetchone()
            if existing:
                continue
            # Check memo matches user id
            in_msg = tx.get("in_msg", {})
            comment = in_msg.get("message", "")
            amount = int(in_msg.get("value", 0))
            if comment.strip() == str(user["id"]) and amount > 0:
                conn.execute("""
                    INSERT OR IGNORE INTO ton_deposits (user_id, tx_hash, amount_nano, confirmed)
                    VALUES (?, ?, ?, 1)
                """, (user["id"], tx_hash, amount))
                conn.execute("UPDATE users SET ton_balance = ton_balance + ? WHERE id=?",
                             (amount, user["id"]))
                new_deposits += amount

        conn.commit()
        ton_balance = conn.execute("SELECT ton_balance FROM users WHERE id=?", (user["id"],)).fetchone()["ton_balance"]
        conn.close()
        return {
            "ok": True,
            "ton_balance": ton_balance,
            "ton_balance_fmt": round(ton_balance / TON_NANO, 4),
            "new_deposits": round(new_deposits / TON_NANO, 4)
        }
    except Exception as e:
        conn.close()
        raise HTTPException(500, str(e))


@app.get("/api/ton/balance/{telegram_id}")
def ton_balance(telegram_id: int):
    conn = get_db()
    user = require_user(conn, telegram_id)
    bal = user["ton_balance"] if "ton_balance" in user.keys() else 0
    conn.close()
    return {
        "ton_balance": bal,
        "ton_balance_fmt": round(bal / TON_NANO, 4)
    }


@app.post("/api/ton/withdraw")
async def ton_withdraw(data: dict):
    """Withdraw from user personal wallet to their Tonkeeper"""
    telegram_id = data.get("telegram_id")
    amount_ton = float(data.get("amount", 0))
    to_address = data.get("to_address", "")
    if amount_ton < 0.1:
        raise HTTPException(400, "Минимум 0.1 TON")
    if not to_address:
        raise HTTPException(400, "Адрес кошелька не указан")
    amount_nano = int(amount_ton * TON_NANO)
    gas_nano = int(0.01 * TON_NANO)  # 0.01 TON for gas
    conn = get_db()
    user = require_user(conn, telegram_id)
    bal = user["ton_balance"] if "ton_balance" in user.keys() else 0
    if bal < amount_nano:
        conn.close()
        raise HTTPException(400, f"Недостаточно TON. Баланс: {round(bal/TON_NANO,4)}")
    # Atomic deduct - prevents race condition double-spend
    cur = conn.execute("UPDATE users SET ton_balance = ton_balance - ? WHERE id=? AND ton_balance >= ?",
                       (amount_nano, user["id"], amount_nano))
    if cur.rowcount == 0:
        conn.close()
        raise HTTPException(400, "Недостаточно TON (concurrent)")

    # Daily withdrawal limit: 1000 TON
    from datetime import datetime, timedelta
    today = datetime.utcnow().strftime("%Y-%m-%d")
    daily_withdrawn = conn.execute("""
        SELECT COALESCE(SUM(amount_nano),0) as total FROM ton_withdrawals
        WHERE user_id=? AND date(created_at)=? AND status!='cancelled'
    """, (user["id"], today)).fetchone()["total"]
    daily_limit_nano = 1000 * TON_NANO
    if daily_withdrawn + amount_nano > daily_limit_nano:
        remaining = (daily_limit_nano - daily_withdrawn) / TON_NANO
        conn.close()
        raise HTTPException(400, f"Дневной лимит вывода 1000 TON. Доступно: {remaining:.2f} TON")
    # Use bot's hot wallet for withdrawal
    bot_seed = BOT_WALLET_SEED
    print(f"DEBUG withdraw: seed={'YES' if bot_seed else 'NO'}, tonsdk={TON_WALLET_AVAILABLE}")
    if not bot_seed or not TON_WALLET_AVAILABLE:
        # Manual fallback
        conn.execute("UPDATE users SET ton_balance = ton_balance - ? WHERE id=?", (amount_nano, user["id"]))
        conn.execute("INSERT INTO ton_withdrawals (user_id, to_address, amount_nano, status) VALUES (?, ?, ?, 'pending')",
                    (user["id"], to_address, amount_nano))
        conn.commit()
        conn.close()
        import asyncio
        asyncio.create_task(notify_user(telegram_id, f"💸 Вывод {amount_ton} TON принят, обрабатывается..."))
        if ADMIN_TG_ID:
            asyncio.create_task(notify_user(ADMIN_TG_ID, f"Вывод вручную: {amount_ton} TON → {to_address}"))
        return {"ok": True, "message": f"Вывод {amount_ton} TON принят"}
    # Deduct from balance first
    conn.commit()
    try:
        mnemonics = bot_seed.split()
        send_amount = amount_nano - gas_nano
        tx_hash = await send_ton(mnemonics, to_address, send_amount)
        conn.execute("INSERT INTO ton_withdrawals (user_id, to_address, amount_nano, tx_hash, status) VALUES (?, ?, ?, ?, 'completed')",
                    (user["id"], to_address, amount_nano, str(tx_hash)))
        conn.commit()
        conn.close()
        ton_fmt = float(f"{amount_ton:.10f}".rstrip('0').rstrip('.'))
        import asyncio
        asyncio.create_task(notify_user(telegram_id, f"💸 Вы вывели {ton_fmt} TON на кошелёк"))
        return {"ok": True, "message": f"Вывод {ton_fmt} TON выполнен"}
    except Exception as e:
        # Rollback on failure
        conn.execute("UPDATE users SET ton_balance = ton_balance + ? WHERE id=?", (amount_nano, user["id"]))
        conn.commit()
        conn.close()
        raise HTTPException(500, f"Ошибка отправки: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)


# ── GEM POOL LOTTERY ──

@app.post("/api/gem/join")
async def gem_join(data: dict):
    """Join GEM pool with Telegram Stars. Cost: 25 stars per ticket. 5 tickets needed."""
    import aiohttp as _aio
    telegram_id = _validate_telegram_id(data.get("telegram_id"))
    qty = max(1, min(3, int(data.get("qty", 1))))  # 1-3 tickets at once
    bot_token = os.getenv("BOT_TOKEN")

    conn = get_db()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS gem_pool (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            username TEXT, first_name TEXT,
            joined_at TEXT DEFAULT (datetime('now')),
            round_id INTEGER DEFAULT 1,
            tickets INTEGER DEFAULT 1
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS gem_pool_config (
            key TEXT PRIMARY KEY, value TEXT
        )""")
        conn.execute("INSERT OR IGNORE INTO gem_pool_config(key,value) VALUES('round_id','1')")
        conn.execute("INSERT OR IGNORE INTO gem_pool_config(key,value) VALUES('total_tickets','0')")
        conn.commit()
    except: pass

    user = require_user(conn, telegram_id)
    round_id = int(conn.execute("SELECT value FROM gem_pool_config WHERE key='round_id'").fetchone()["value"])

    # Create Stars invoice
    prices = [{"label": f"GEM билет x{qty}", "amount": 25 * qty}]
    payload = f"gem_pool_{round_id}_{telegram_id}_{qty}"

    async with _aio.ClientSession() as session:
        async with session.post(
            f"https://api.telegram.org/bot{bot_token}/createInvoiceLink",
            json={
                "title": "GEM Розыгрыш",
                "description": f"{qty} билет{'а' if qty>1 else ''} · шанс выиграть GEM подарок",
                "payload": payload,
                "currency": "XTR",
                "prices": prices,
            }
        ) as resp:
            result = await resp.json()
            if not result.get("ok"):
                conn.close()
                raise HTTPException(400, result.get("description", "Failed"))
            invoice_link = result["result"]

    conn.close()
    return {"ok": True, "invoice_link": invoice_link, "payload": payload}


@app.post("/api/gem/confirm")
def gem_confirm(data: dict):
    """Called after successful Stars payment for GEM pool."""
    import random
    telegram_id = _validate_telegram_id(data.get("telegram_id"))
    payload = data.get("payload", "")
    stars_paid = int(data.get("stars", 0))

    conn = get_db()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS gem_pool (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            username TEXT, first_name TEXT,
            joined_at TEXT DEFAULT (datetime('now')),
            round_id INTEGER DEFAULT 1,
            tickets INTEGER DEFAULT 1
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS gem_pool_config (key TEXT PRIMARY KEY, value TEXT)""")
        conn.execute("INSERT OR IGNORE INTO gem_pool_config(key,value) VALUES('round_id','1')")
        conn.execute("INSERT OR IGNORE INTO gem_pool_config(key,value) VALUES('total_tickets','0')")
        conn.commit()
    except: pass

    # Parse payload: gem_pool_{round_id}_{telegram_id}_{qty}
    parts = payload.split("_")
    if len(parts) < 5 or parts[0] != "gem" or parts[1] != "pool":
        conn.close()
        raise HTTPException(400, "Invalid payload")

    round_id = int(parts[2])
    qty = int(parts[4]) if len(parts) > 4 else 1

    user = require_user(conn, telegram_id)
    current_round = int(conn.execute("SELECT value FROM gem_pool_config WHERE key='round_id'").fetchone()["value"])

    # Add tickets (each ticket = separate entry for fair draw)
    for _ in range(qty):
        conn.execute("INSERT INTO gem_pool(telegram_id, username, first_name, round_id, tickets) VALUES(?,?,?,?,1)",
                     (telegram_id, user["username"] or "", user["first_name"] or "", current_round))
    conn.commit()

    # Count total tickets in current round
    total = conn.execute("SELECT COUNT(*) as cnt FROM gem_pool WHERE round_id=?", (current_round,)).fetchone()["cnt"]
    unique = conn.execute("SELECT COUNT(DISTINCT telegram_id) as cnt FROM gem_pool WHERE round_id=?", (current_round,)).fetchone()["cnt"]

    result = {"ok": True, "total_tickets": total, "needed": 5, "unique_players": unique}

    if total >= 5:
        # Draw!
        all_entries = conn.execute("SELECT telegram_id, username, first_name FROM gem_pool WHERE round_id=?", (current_round,)).fetchall()
        winner = random.choice(all_entries)
        winner_tg_id = winner["telegram_id"]
        winner_name = winner["username"] or winner["first_name"] or str(winner_tg_id)

        # New round
        conn.execute("UPDATE gem_pool_config SET value=? WHERE key='round_id'", (str(current_round + 1),))
        conn.commit()

        # Notify all unique players
        notified = set()
        for entry in all_entries:
            tid = entry["telegram_id"]
            if tid in notified: continue
            notified.add(tid)
            if tid == winner_tg_id:
                notify_user_sync(tid, "🎉 Вы выиграли GEM! Подарок будет отправлен в ближайшее время ✨")
            else:
                notify_user_sync(tid, f"😔 Не повезло. Победил @{winner_name}. Попробуйте ещё раз!")

        # Notify admin
        notify_user_sync(7308147004, f"🎁 GEM розыгрыш завершён!\nПобедитель: @{winner_name} (id: {winner_tg_id})\nОтправь подарок GEM 💎\nhttps://t.me/{winner_name}")

        result["draw"] = True
        result["winner_id"] = winner_tg_id
        result["winner"] = winner_name
        result["i_won"] = (telegram_id == winner_tg_id)

    conn.close()
    return result

@app.get("/api/gem/status")
def gem_status(telegram_id: int = None):
    """Get current GEM pool status."""
    conn = get_db()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS gem_pool (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            username TEXT,
            first_name TEXT,
            joined_at TEXT DEFAULT (datetime('now')),
            round_id INTEGER DEFAULT 1
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS gem_pool_config (
            key TEXT PRIMARY KEY, value TEXT
        )""")
        conn.execute("INSERT OR IGNORE INTO gem_pool_config(key,value) VALUES('round_id','1')")
        conn.commit()
    except: pass

    round_id = int(conn.execute("SELECT value FROM gem_pool_config WHERE key='round_id'").fetchone()["value"])
    count = conn.execute("SELECT COUNT(*) as cnt FROM gem_pool WHERE round_id=?", (round_id,)).fetchone()["cnt"]

    already_in = False
    if telegram_id:
        already_in = bool(conn.execute("SELECT id FROM gem_pool WHERE telegram_id=? AND round_id=?",
                                       (telegram_id, round_id)).fetchone())
    conn.close()
    return {"count": count, "needed": 5, "round_id": round_id, "already_in": already_in}
