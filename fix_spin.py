with open('/root/memstroy/api.py', 'r', encoding='utf-8') as f:
    src = f.read()

old = '''    roll = random.randint(1, 100)
    if roll == 1:
        prize = 100
        combo = "777"
    elif roll <= 3:
        prize = 10
        combo = "cherry"
    else:
        prize = 1
        combo = "star"'''

new = '''    roll = random.randint(1, 100)
    if roll == 1:
        prize = 100
        combo = "777"
    elif roll <= 3:
        prize = 50
        combo = "watermelon"
    elif roll <= 6:
        prize = 10
        combo = "grape"
    elif roll <= 10:
        prize = 5
        combo = "lemon"
    else:
        prize = 1
        combo = "cherry"'''

if old in src:
    src = src.replace(old, new)
    print("OK prizes fixed")
else:
    print("NOT FOUND - trying alternative...")
    # попробуем найти что есть
    import re
    m = re.search(r'roll = random\.randint\(1, 100\).*?combo = "\w+"', src, re.DOTALL)
    if m:
        print("Found:", repr(m.group()[:200]))

with open('/root/memstroy/api.py', 'w', encoding='utf-8') as f:
    f.write(src)

print("Done")
