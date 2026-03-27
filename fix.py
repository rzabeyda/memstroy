content = open('/root/memstroy/api.py').read()

fixes = [
    ('combo="str_flush"; mult=100', 'combo="str_flush"; mult=200'),
    ('combo="4_of_kind"; mult=40', 'combo="4_of_kind"; mult=50'),
    ('combo="full_house"; mult=12', 'combo="full_house"; mult=15'),
    ('combo="flush"; mult=9', 'combo="flush"; mult=10'),
    ('combo="straight"; mult=7', 'combo="straight"; mult=8'),
]

for old, new in fixes:
    if old in content:
        content = content.replace(old, new, 1)
        print(f'OK: {old[:30]}')
    else:
        print(f'SKIP (already fixed?): {old[:30]}')

open('/root/memstroy/api.py', 'w').write(content)
print('Done')
