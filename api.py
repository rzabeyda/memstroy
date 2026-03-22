from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional, Union
import sqlite3
import random
import string
import os
from database import get_db, init_db

app = FastAPI(title="MemStroy API")

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
        pass
    return 0

import aiohttp as _aiohttp

async def notify_user(telegram_id: int, text: str):
    """Send Telegram notification to user"""
    bot_token = os.getenv("BOT_TOKEN","")
    if not bot_token: return
    try:
        async with _aiohttp.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={"chat_id": telegram_id, "text": text}
            )
    except:
        pass

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest

class NgrokMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        response = await call_next(request)
        response.headers["ngrok-skip-browser-warning"] = "true"
        return response

app.add_middleware(NgrokMiddleware)

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
        pass
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
                        await notify_user(seller["telegram_id"], f"✅ {card_name} продана за {price_ton} TON")
                else:
                    seller = conn.execute("SELECT telegram_id FROM users WHERE id=?", (auction["seller_id"],)).fetchone()
                    card = conn.execute("SELECT cd.name FROM user_cards uc JOIN card_definitions cd ON uc.card_def_id=cd.id WHERE uc.id=?", (auction["user_card_id"],)).fetchone()
                    if seller and card:
                        await notify_user(seller["telegram_id"], f"Аукцион без ставок. {card['name']} возвращена")
            if expired:
                conn.commit()
            conn.close()
        except Exception as e:
            print(f"Auction background error: {e}")


@app.get("/")
def root():
    return FileResponse("webapp/index.html")


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
        conn.close()
        return {"ok": True, "user_id": existing["id"], "new": False}

    ref_code = gen_ref_code()
    referred_by = None

    if data.ref_code:
        referrer = conn.execute("SELECT * FROM users WHERE ref_code=?", (data.ref_code,)).fetchone()
        if referrer:
            referred_by = referrer["id"]

    # Generate personal TON wallet for this user
    wallet_address = ""
    wallet_mnemonic = ""
    if TON_WALLET_AVAILABLE:
        try:
            mnemonics, address = generate_wallet()
            wallet_address = address
            wallet_mnemonic = json.dumps(mnemonics)
        except Exception as e:
            print(f"Wallet gen error: {e}")

    conn.execute("""
        INSERT INTO users (telegram_id, username, first_name, last_name, ref_code, referred_by, wallet_address, wallet_mnemonic)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (data.telegram_id, data.username, data.first_name, data.last_name, ref_code, referred_by, wallet_address, wallet_mnemonic))
    conn.commit()
    user_id = conn.execute("SELECT id FROM users WHERE telegram_id=?", (data.telegram_id,)).fetchone()["id"]
    conn.close()
    return {"ok": True, "user_id": user_id, "new": True}


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
        "cards": [dict(c) for c in cards]
    }


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
    parts = data.payload.split("_")
    action = parts[0]

    if action == "buy" and parts[1] == "card":
        collection_id = int(parts[2])
        qty = int(parts[3]) if len(parts) > 3 else 1
        cards = []
        for _ in range(qty):
            chosen = _buy_card(conn, user["id"], collection_id)
            cards.append(chosen)
        _pay_referral_bonus(conn, user["id"], data.stars)
        # Track stars spent for leaderboard
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

    elif action == "transfer" and parts[1] == "fee":
        card_id = int(parts[2])
        to_id_raw = parts[3]
        # Find recipient
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
                VALUES (?, ?, ?, 'transfer', 1)
            """, (user["id"], to_user["id"], card_id))
            conn.commit()
            conn.close()
            recipient = to_user["first_name"] or to_user["username"] or "friend"
            return {"message": f"✈️ Card sent to {recipient}!"}
        except Exception as e:
            conn.close()
            return {"message": "Card transferred!"}

    conn.close()
    return {"message": "Payment processed"}


def _pay_referral_bonus(conn, user_id, amount):
    """10% referral bonus"""
    user = conn.execute("SELECT referred_by FROM users WHERE id=?", (user_id,)).fetchone()
    if not user or not user["referred_by"]:
        return
    bonus = max(1, int(amount * 0.20))
    conn.execute("UPDATE users SET stars_balance = stars_balance + ? WHERE id=?",
                 (bonus, user["referred_by"]))
    conn.execute("""
        INSERT INTO referral_bonuses (referrer_id, referred_id, bonus_stars)
        VALUES (?, ?, ?)
    """, (user["referred_by"], user_id, bonus))
    conn.commit()


def _buy_card(conn, user_id, collection_id):
    col = conn.execute("SELECT * FROM collections WHERE id=? AND remaining > 0", (collection_id,)).fetchone()
    if not col:
        raise HTTPException(400, "Collection sold out")

    serial = col["total_supply"] - col["remaining"] + 1

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
    conn.execute("UPDATE collections SET remaining = remaining - 1 WHERE id=?", (collection_id,))
    conn.execute("""
        INSERT INTO transactions (from_user_id, to_user_id, type, stars_amount)
        VALUES (?, ?, 'buy', ?)
    """, (user_id, user_id, col["base_price"]))
    conn.commit()
    return dict(chosen)


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
        VALUES (?, ?, ?, 'upgrade', 0)
    """, (user["id"], user["id"], user_card_id))
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
        LIMIT 100
    """, (user["id"], user["id"])).fetchall()
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
        asyncio.create_task(notify_user(seller_row["telegram_id"], f"Ваша карточка {card_nm} продана за {price_ton} TON"))
    asyncio.create_task(notify_user(buyer_tg, f"Вы купили карточку {card_nm} за {price_ton} TON"))
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
    conn.close()
    import asyncio
    if seller_row:
        asyncio.create_task(notify_user(seller_row["telegram_id"],
            f"Вашу карточку Ponki · {card_nm} #{serial} купили за {price_ton} TON"))
    asyncio.create_task(notify_user(buyer_tg,
        f"Вы купили Ponki · {card_nm} #{serial} за {price_ton} TON"))
    return {"ok": True, "message": "Card purchased!"}


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
    from_name = from_user["first_name"] or from_user["username"] or "Someone"
    card_row = conn.execute("SELECT cd.name FROM user_cards uc JOIN card_definitions cd ON uc.card_def_id=cd.id WHERE uc.id=?", (data.user_card_id,)).fetchone()
    card_name = card_row["name"] if card_row else "карточка"
    conn.close()
    import asyncio
    asyncio.create_task(notify_user(to_tg_id, f"{from_name} передал вам карточку {card_name}"))
    asyncio.create_task(notify_user(data.from_telegram_id, f"Карточка {card_name} отправлена"))
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
    # Reserve (lock) the amount
    conn.execute("UPDATE users SET ton_balance = ton_balance - ? WHERE id=?", (amount_nano, from_user["id"]))
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
    asyncio.create_task(notify_user(
        card["owner_tg"],
        f"Вам предложили {amount_ton} TON за карточку {card['card_name']}"
    ))
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
    asyncio.create_task(notify_user(offer["buyer_tg"], f"Ваш оффер принят! Карточка {offer['card_name']} ваша за {price_ton} TON"))
    asyncio.create_task(notify_user(owner["telegram_id"], f"Вы приняли оффер {price_ton} TON за {offer['card_name']}"))
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

    # Reserve new bid
    conn.execute("UPDATE users SET ton_balance = ton_balance - ? WHERE id=?",
                (amount_nano, bidder["id"]))

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
        asyncio.create_task(notify_user(seller["telegram_id"],
            f"Новая ставка {amount_ton} TON на {card_name} от {bidder_name}"))
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
                asyncio.create_task(notify_user(winner["telegram_id"],
                    f"🏆 Вы выиграли аукцион! {card_name} ваша за {price_ton} TON"))
            if seller:
                asyncio.create_task(notify_user(seller["telegram_id"],
                    f"✅ Аукцион завершён. {card_name} продана за {price_ton} TON"))
        else:
            # No bids - return card to seller
            conn.execute("UPDATE user_cards SET is_listed=0 WHERE id=?", (auction["user_card_id"],))
            seller_row = conn.execute("SELECT telegram_id FROM users WHERE id=?", (auction["seller_id"],)).fetchone()
            card_row = conn.execute("SELECT cd.name FROM user_cards uc JOIN card_definitions cd ON uc.card_def_id=cd.id WHERE uc.id=?", (auction["user_card_id"],)).fetchone()
            if seller_row and card_row:
                import asyncio
                asyncio.create_task(notify_user(seller_row["telegram_id"],
                    f"Аукцион завершён без ставок. {card_row['name']} возвращена вам"))

    conn.commit()
    conn.close()
    return {"ok": True, "finished": len(expired)}


@app.get("/api/stars_link")
def stars_link():
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
    else:
        payload = f"buy_card_{collection_id}_{qty}"
        title = "Ponki Card Pack"
        description = f"Open {qty} Ponki card pack{'s' if qty > 1 else ''}!"
        prices = [{"label": f"Ponki Card x{qty}", "amount": qty}]

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
                raise HTTPException(400, result.get("description", "Failed to create invoice"))
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
def leaderboard(telegram_id: int = None):
    conn = get_db()
    top = conn.execute("""
        SELECT telegram_id, username, first_name,
               COALESCE(stars_spent, 0) as stars_spent,
               COALESCE(ton_spent, 0) as ton_spent
        FROM users
        ORDER BY (COALESCE(stars_spent, 0) + COALESCE(ton_spent, 0)/1000000000) DESC
        LIMIT 100
    """).fetchall()

    my_rank = None
    my_entry = None
    if telegram_id:
        rank_row = conn.execute("""
            SELECT COUNT(*) as cnt FROM users
            WHERE (COALESCE(stars_spent, 0) + COALESCE(ton_spent, 0)/1000000000) > (
                SELECT (COALESCE(stars_spent, 0) + COALESCE(ton_spent, 0)/1000000000)
                FROM users WHERE telegram_id=?
            )
        """, (telegram_id,)).fetchone()
        if rank_row:
            my_rank = rank_row["cnt"] + 1
        user_row = conn.execute(
            "SELECT telegram_id, username, first_name, stars_spent, ton_spent FROM users WHERE telegram_id=?",
            (telegram_id,)
        ).fetchone()
        if user_row:
            my_entry = dict(user_row)

    conn.close()
    return {
        "top": [dict(r) for r in top],
        "my_rank": my_rank,
        "my_entry": my_entry
    }



# ── TON SYSTEM ──
BOT_TON_ADDRESS = os.getenv("BOT_WALLET_ADDRESS", "UQDngkmwbJxausCBgrbXcS_LmQYtGLG0-qfsaCYijyczQVap")
BOT_WALLET_SEED = os.getenv("BOT_WALLET_SEED", "")
ADMIN_TG_ID = int(os.getenv("ADMIN_TG_ID", "0"))
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
    asyncio.create_task(notify_user(telegram_id, f"Пополнение {ton_fmt} TON зачислено"))
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
        asyncio.create_task(notify_user(telegram_id, f"Вывод {amount_ton} TON принят, обрабатывается"))
        if ADMIN_TG_ID:
            asyncio.create_task(notify_user(ADMIN_TG_ID, f"Вывод вручную: {amount_ton} TON → {to_address}"))
        return {"ok": True, "message": f"Вывод {amount_ton} TON принят"}
    # Deduct from balance first
    conn.execute("UPDATE users SET ton_balance = ton_balance - ? WHERE id=?", (amount_nano, user["id"]))
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
        asyncio.create_task(notify_user(telegram_id, f"Вы вывели {ton_fmt} TON"))
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
