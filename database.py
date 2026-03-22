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
            total_supply INTEGER DEFAULT 10000,
            remaining INTEGER DEFAULT 10000,
            base_price INTEGER DEFAULT 100,
            upgrade_price INTEGER DEFAULT 50,
            is_active INTEGER DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Card definitions
    cur.execute("""
        CREATE TABLE IF NOT EXISTS card_definitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collection_id INTEGER REFERENCES collections(id),
            name TEXT NOT NULL,
            image_url TEXT NOT NULL,
            description TEXT DEFAULT '',
            drop_weight INTEGER DEFAULT 100
        )
    """)

    # User cards
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(id),
            card_def_id INTEGER REFERENCES card_definitions(id),
            collection_id INTEGER REFERENCES collections(id),
            serial_number INTEGER NOT NULL,
            is_upgraded INTEGER DEFAULT 0,
            is_listed INTEGER DEFAULT 0,
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
            type TEXT NOT NULL,
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

    # TON deposits
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ton_deposits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(id),
            tx_hash TEXT UNIQUE NOT NULL,
            amount_nano INTEGER NOT NULL,
            confirmed INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # TON withdrawals
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ton_withdrawals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(id),
            to_address TEXT NOT NULL,
            amount_nano INTEGER NOT NULL,
            tx_hash TEXT DEFAULT '',
            status TEXT DEFAULT 'pending',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Add ton_balance column to users if not exists
    try:
        cur.execute("ALTER TABLE users ADD COLUMN ton_balance INTEGER DEFAULT 0")
    except:
        pass

    # Custodial wallet columns
    try:
        cur.execute("ALTER TABLE users ADD COLUMN wallet_address TEXT DEFAULT ''")
    except:
        pass
    try:
        cur.execute("ALTER TABLE users ADD COLUMN wallet_mnemonic TEXT DEFAULT ''")
    except:
        pass

    # Add cashback_balance column
    try:
        cur.execute("ALTER TABLE users ADD COLUMN cashback_balance INTEGER DEFAULT 0")
    except:
        pass

    # Add stars_spent and ton_spent columns for leaderboard
    try:
        cur.execute("ALTER TABLE users ADD COLUMN stars_spent INTEGER DEFAULT 0")
    except:
        pass
    try:
        cur.execute("ALTER TABLE users ADD COLUMN ton_spent INTEGER DEFAULT 0")
    except:
        pass

    # Seed Ponki collection if empty
    cur.execute("SELECT COUNT(*) FROM collections")
    if cur.fetchone()[0] == 0:
        cur.execute("""
            INSERT INTO collections (name, description, total_supply, remaining, base_price, upgrade_price)
            VALUES ('Ponki', 'The first Ponki collection. 50 unique models.', 10000, 10000, 100, 0)
        """)
        collection_id = cur.lastrowid

        # All 50 Ponki models
        # drop_weight = percent * 10 (e.g. 0.5% = 5, 1% = 10, 3% = 30)
        cards = [
            # 0.5%
            ('Plush Frog',      '/static/ponki/ponki_pepe.png',     5),
            ('Insta Girl',      '/static/ponki/ponki_lui.png',      5),
            ('Lambo',           '/static/ponki/ponki_lambo.png',    5),
            # 1%
            ('BTC',             '/static/ponki/ponki_btc.png',      10),
            ('Gelik',           '/static/ponki/ponki_mers.png',     10),
            ('Frosty',          '/static/ponki/ponki_elsa.png',     10),
            ('Storm',           '/static/ponki/ponki_storm.png',    10),
            ('Throne',          '/static/ponki/ponki_tron.png',     10),
            ('Rich',            '/static/ponki/ponki_money.png',    10),
            ('MM',              '/static/ponki/ponki_masha.png',    10),
            ('Utya',            '/static/ponki/ponki_utya.png',     10),
            # 1.5%
            ('Vintage Blonde',  '/static/ponki/ponki_monro.png',    15),
            ('Pop Star',        '/static/ponki/ponki_gaga.png',     15),
            ('Cat',             '/static/ponki/ponki_cat.png',      15),
            ('Bumer',           '/static/ponki/ponki_bmw.png',      15),
            ('Joker',           '/static/ponki/ponki_joker.png',    15),
            # 2%
            ('Squid',           '/static/ponki/ponki_igra.png',     20),
            ('Poker',           '/static/ponki/ponki_poker.png',    20),
            ('Coder',           '/static/ponki/ponki_matrix.png',   20),
            ('Spa',             '/static/ponki/ponki_spa.png',      20),
            ('Roses',           '/static/ponki/ponki_rose.png',     20),
            ('Four',            '/static/ponki/ponki_4.png',        20),
            ('Alisa',           '/static/ponki/ponki_alisa.png',    20),
            ('Dober',           '/static/ponki/ponke_dob.png',      20),
            ('Chaos Girl',      '/static/ponki/ponki_har.png',      20),
            ('Fitonyashka',     '/static/ponki/ponki_gym.png',      20),
            ('Mermaid',         '/static/ponki/ponki_merm.png',     20),
            ('Tomber',          '/static/ponki/ponki_tomb.png',     20),
            ('Cat Lady',        '/static/ponki/ponki_catwoman.png', 20),
            ('School Girl',     '/static/ponki/ponki_school.png',   20),
            ('Business Woman',  '/static/ponki/ponki_biz.png',      20),
            # 2.5%
            ('Archer',          '/static/ponki/ponki_kat.png',      25),
            ('Dark',            '/static/ponki/ponki_male.png',     25),
            ('Halloween',       '/static/ponki/ponki_hell.png',     25),
            ('Wednesday',       '/static/ponki/ponki_wen.png',      25),
            ('Red Cap',         '/static/ponki/ponki_redcap.png',   25),
            ('Fighter',         '/static/ponki/ponki_ufc.png',      25),
            ('Delta',           '/static/ponki/ponki_war.png',      25),
            ('Rap',             '/static/ponki/ponki_rap.png',      25),
            ('Delivery',        '/static/ponki/ponki_div.png',      25),
            ('Nurse',           '/static/ponki/ponki_medic.png',    25),
            # 3%
            ('Love',            '/static/ponki/ponki_love.png',     30),
            ('Dress',           '/static/ponki/ponki_dress.png',    30),
            ('Retro',           '/static/ponki/ponki_80.png',       30),
            ('Singer',          '/static/ponki/ponki_sing.png',     30),
            ('Hero',            '/static/ponki/ponki_wond.png',     30),
            ('Beauty',          '/static/ponki/ponki_samka.png',    30),
            ('Red',             '/static/ponki/ponki_red.png',      30),
            ('Cleo',            '/static/ponki/ponki_cleo.png',     30),
            ('Street',          '/static/ponki/ponki_adidas.png',   30),
        ]

        for name, image_url, weight in cards:
            cur.execute("""
                INSERT INTO card_definitions (collection_id, name, image_url, drop_weight)
                VALUES (?, ?, ?, ?)
            """, (collection_id, name, image_url, weight))

    # Offers table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_user_id INTEGER REFERENCES users(id),
            to_user_id INTEGER REFERENCES users(id),
            user_card_id INTEGER REFERENCES user_cards(id),
            amount_nano INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()
    print("✅ Database initialized")


if __name__ == "__main__":
    init_db()
