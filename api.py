from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import sqlite3
import random
import string
import os
from database import get_db, init_db

app = FastAPI(title="MemStroy API")

app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/webapp", StaticFiles(directory="webapp"), name="webapp")


@app.on_event("startup")
def startup():
    init_db()
    os.makedirs("static/ponki", exist_ok=True)


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
    price_stars: int


class BuyListing(BaseModel):
    telegram_id: int
    listing_id: int


class TransferCard(BaseModel):
    from_telegram_id: int
    to_telegram_id: int
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

    conn.execute("""
        INSERT INTO users (telegram_id, username, first_name, last_name, ref_code, referred_by)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (data.telegram_id, data.username, data.first_name, data.last_name, ref_code, referred_by))
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
        chosen = _buy_card(conn, user["id"], collection_id)
        conn.close()
        return {"message": f"🃏 You got: {chosen['name']}!", "card": chosen}

    elif action == "addstars":
        amount = int(parts[1])
        conn.execute("UPDATE users SET stars_balance = stars_balance + ? WHERE id=?", (amount, user["id"]))
        conn.commit()
        conn.close()
        return {"message": f"⭐ {amount} Stars added!"}

    conn.close()
    return {"message": "Payment processed"}


def _pay_referral_bonus(conn, user_id, amount):
    """10% referral bonus"""
    user = conn.execute("SELECT referred_by FROM users WHERE id=?", (user_id,)).fetchone()
    if not user or not user["referred_by"]:
        return
    bonus = max(1, int(amount * 0.10))
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
                 (data.price_stars, data.user_card_id))
    conn.execute("""
        INSERT INTO market_listings (user_card_id, seller_id, price_stars)
        VALUES (?, ?, ?)
    """, (data.user_card_id, user["id"], data.price_stars))
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
        raise HTTPException(404, "Listing not found")
    if listing["seller_id"] == buyer["id"]:
        raise HTTPException(400, "Cannot buy your own card")
    if buyer["stars_balance"] < listing["price_stars"]:
        raise HTTPException(400, "Not enough stars")

    price = listing["price_stars"]
    commission = max(1, int(price * 0.05))  # 5% commission
    seller_gets = price - commission

    conn.execute("UPDATE users SET stars_balance = stars_balance - ? WHERE id=?", (price, buyer["id"]))
    conn.execute("UPDATE users SET stars_balance = stars_balance + ? WHERE id=?", (seller_gets, listing["seller_id"]))
    conn.execute("UPDATE user_cards SET user_id=?, is_listed=0 WHERE id=?", (buyer["id"], listing["user_card_id"]))
    conn.execute("UPDATE market_listings SET is_active=0 WHERE id=?", (listing["id"],))
    conn.execute("""
        INSERT INTO transactions (from_user_id, to_user_id, user_card_id, type, stars_amount)
        VALUES (?, ?, ?, 'market_buy', ?)
    """, (buyer["id"], listing["seller_id"], listing["user_card_id"], price))
    conn.commit()
    conn.close()
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
    to_user = require_user(conn, data.to_telegram_id)

    TRANSFER_FEE = 1  # 1 star

    if from_user["stars_balance"] < TRANSFER_FEE:
        raise HTTPException(400, "Need 1 ⭐ to transfer")

    card = conn.execute("""
        SELECT * FROM user_cards WHERE id=? AND user_id=? AND is_listed=0
    """, (data.user_card_id, from_user["id"])).fetchone()
    if not card:
        raise HTTPException(404, "Card not found")

    conn.execute("UPDATE users SET stars_balance = stars_balance - ? WHERE id=?",
                 (TRANSFER_FEE, from_user["id"]))
    conn.execute("UPDATE user_cards SET user_id=?, transferred_count = transferred_count + 1 WHERE id=?",
                 (to_user["id"], data.user_card_id))
    conn.execute("""
        INSERT INTO transactions (from_user_id, to_user_id, user_card_id, type, stars_amount)
        VALUES (?, ?, ?, 'transfer', ?)
    """, (from_user["id"], to_user["id"], data.user_card_id, TRANSFER_FEE))
    conn.commit()
    conn.close()
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


@app.get("/api/stars_link")
def stars_link():
    return {"url": "https://t.me/stars"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
