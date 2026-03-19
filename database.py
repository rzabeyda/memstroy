import sqlite3
import os

DB_PATH = os.getenv("DB_PATH", "memstroy.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_db()
    cur = conn.cursor()

    # Users
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE NOT NULL,
            username TEXT DEFAULT '',
            first_name TEXT DEFAULT '',
            last_name TEXT DEFAULT '',
            stars_balance INTEGER DEFAULT 0,
            ref_code TEXT UNIQUE,
            referred_by INTEGER REFERENCES users(id),
            ton_address TEXT DEFAULT '',
            dev_tokens INTEGER DEFAULT 10000,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Collections
    cur.execute("""
        CREATE TABLE IF NOT EXISTS collections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            total_supply INTEGER DEFAULT 1000,
            remaining INTEGER DEFAULT 1000,
            base_price INTEGER DEFAULT 25,
            is_active INTEGER DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Card definitions (base + upgrade skins per collection)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_definitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collection_id INTEGER REFERENCES collections(id),
            rarity TEXT DEFAULT 'base',  -- base, rare, epic, legendary
            name TEXT NOT NULL,
            image_url TEXT NOT NULL,
            description TEXT DEFAULT '',
            drop_weight INTEGER DEFAULT 100  -- higher = more common
        )
    """)

    # User cards (actual owned NFTs)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(id),
            card_def_id INTEGER REFERENCES card_definitions(id),
            collection_id INTEGER REFERENCES collections(id),
            serial_number INTEGER NOT NULL,  -- unique number in collection
            is_upgraded INTEGER DEFAULT 0,
            is_listed INTEGER DEFAULT 0,  -- listed on market
            list_price INTEGER DEFAULT 0,
            transferred_count INTEGER DEFAULT 0,
            acquired_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(collection_id, serial_number)
        )
    """)

    # Market listings
    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_card_id INTEGER REFERENCES user_cards(id),
            seller_id INTEGER REFERENCES users(id),
            price_stars INTEGER NOT NULL,
            is_active INTEGER DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Transactions log
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_user_id INTEGER REFERENCES users(id),
            to_user_id INTEGER REFERENCES users(id),
            user_card_id INTEGER REFERENCES user_cards(id),
            type TEXT NOT NULL,  -- buy, upgrade, transfer, sell, ref_bonus
            stars_amount INTEGER DEFAULT 0,
            payload TEXT DEFAULT '',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Referral bonuses log
    cur.execute("""
        CREATE TABLE IF NOT EXISTS referral_bonuses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER REFERENCES users(id),
            referred_id INTEGER REFERENCES users(id),
            bonus_stars INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Seed first collection if empty
    cur.execute("SELECT COUNT(*) FROM collections")
    if cur.fetchone()[0] == 0:
        cur.execute("""
            INSERT INTO collections (name, description, total_supply, remaining, base_price)
            VALUES ('MemStroy', 'The first MemStroy collection.', 1000, 1000, 25)
        """)
        collection_id = cur.lastrowid

        # Base card (everyone gets this first)
        cur.execute("""
            INSERT INTO card_definitions (collection_id, rarity, name, image_url, description, drop_weight)
            VALUES (?, 'base', 'MemStroy Card', '/static/cards/1.jpg', 'The original MemStroy card.', 0)
        """, (collection_id,))

        # Upgrade results (random skin after upgrade)
        skins = [
            ('rare', 'MemStroy Red', '/static/cards/2.jpg', 'Rare edition.', 50),
            ('rare', 'MemStroy Blue', '/static/cards/3.jpg', 'Rare edition.', 50),
            ('epic', 'MemStroy Green', '/static/cards/4.jpg', 'Epic edition.', 25),
            ('epic', 'MemStroy Purple', '/static/cards/5.jpg', 'Epic edition.', 25),
            ('legendary', 'MemStroy Gold', '/static/cards/6.jpg', 'Legendary edition.', 10),
            ('legendary', 'MemStroy Holo', '/static/cards/7.jpg', 'Legendary edition.', 5),
        ]
        for rarity, name, img, desc, weight in skins:
            cur.execute("""
                INSERT INTO card_definitions (collection_id, rarity, name, image_url, description, drop_weight)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (collection_id, rarity, name, img, desc, weight))

    conn.commit()
    conn.close()
    print("✅ Database initialized")


if __name__ == "__main__":
    init_db()
