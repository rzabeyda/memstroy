content = open('/root/memstroy/api.py').read()

old = '''    roll = random.random()
    # 5% chance of joker = auto win
    if roll < 0.05:
        result = choice  # joker = player wins regardless
        card = {"r": "Jo", "s": "🃏"}
        won = True
        new_amount = amount * 2
        conn.execute("UPDATE users SET gems = gems + ? WHERE id=?", (amount, user["id"]))
        conn.commit()
        gems_after = conn.execute("SELECT gems FROM users WHERE id=?", (user["id"],)).fetchone()["gems"]
        conn.close()
        return {"ok": True, "result": result, "card": card, "won": True, "new_amount": new_amount, "gems": gems_after}
    # Normal draw
    if roll < 0.05 + win_prob:
        result = choice  # player wins
    else:
        result = "red" if choice == "black" else "black"  # player loses
    if result == "red":
        card = {"r": random.choice(["2","3","4","5","6","7","8","9","10","J","Q","K","A"]),
                "s": random.choice(suits_red)}
    else:
        card = {"r": random.choice(["2","3","4","5","6","7","8","9","10","J","Q","K","A"]),
                "s": random.choice(suits_black)}
    won = result == choice
    if won:
        new_amount = amount * 2
        conn.execute("UPDATE users SET gems = gems + ? WHERE id=?", (amount, user["id"]))
        conn.commit()
    else:
        new_amount = 0
        conn.execute("UPDATE users SET gems = CASE WHEN gems >= ? THEN gems - ? ELSE 0 END WHERE id=?", (amount, amount, user["id"]))
        conn.commit()'''

new = '''    # Real deck: 52 cards + 1 joker
    ranks = ["2","3","4","5","6","7","8","9","10","J","Q","K","A"]
    deck = [{"r": r, "s": s} for s in (suits_red + suits_black) for r in ranks]
    deck.append({"r": "Jo", "s": "joker"})
    random.shuffle(deck)
    card = deck[0]

    is_joker = card["r"] == "Jo"
    if is_joker:
        result = choice
        won = True
    else:
        result = "red" if card["s"] in suits_red else "black"
        won = result == choice
        # House edge: 6% flip on win
        if won and random.random() < 0.06:
            won = False
            result = "red" if choice == "black" else "black"

    if won:
        new_amount = amount * 2
        conn.execute("UPDATE users SET gems = gems + ? WHERE id=?", (amount, user["id"]))
        conn.commit()
    else:
        new_amount = 0
        conn.execute("UPDATE users SET gems = CASE WHEN gems >= ? THEN gems - ? ELSE 0 END WHERE id=?", (amount, amount, user["id"]))
        conn.commit()'''

if old in content:
    content = content.replace(old, new, 1)
    print("OK: double fixed - real deck, single credit path")
else:
    print("NOT FOUND")

open('/root/memstroy/api.py', 'w').write(content)
