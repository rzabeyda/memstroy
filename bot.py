import asyncio
import logging
import json
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command

ADMIN_ID = 7308147004
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, LabeledPrice, PreCheckoutQuery
import os
import aiohttp
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://yourdomain.com")
API_URL = os.getenv("API_URL", "http://localhost:8000")

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


@dp.message(CommandStart())
async def start(message: types.Message):
    ref_code = None
    if message.text and len(message.text.split()) > 1:
        ref_code = message.text.split()[1]

    name = message.from_user.first_name or message.from_user.username or "friend"

    # Регистрируем пользователя
    async with aiohttp.ClientSession() as session:
        payload = {
            "telegram_id": message.from_user.id,
            "username": message.from_user.username or "",
            "first_name": message.from_user.first_name or "",
            "last_name": message.from_user.last_name or "",
            "ref_code": ref_code
        }
        try:
            async with session.post(f"{API_URL}/api/register", json=payload) as resp:
                pass
        except:
            pass

    # Проверяем участие в розыгрыше
    if ref_code and ref_code.startswith("giveaway"):
        giveaway_key = ref_code
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(f"{API_URL}/api/simple_giveaway/join", json={
                    "telegram_id": message.from_user.id,
                    "giveaway_key": giveaway_key
                }) as resp:
                    data = await resp.json(content_type=None)
                participants = data.get("participants", 0)
                await message.answer(
                    f"✅ {name}, ты зарегистрирован в розыгрыше!\n\n"
                    f"👥 Участников сейчас: <b>{participants}</b>\n"
                    f"⏰ Итоги через 24 часа\n\n"
                    f"🎰 Пока жди — играй в Memstroy!",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="🦄 Открыть Memstroy", web_app=WebAppInfo(url=WEBAPP_URL))
                    ]])
                )
                return
            except Exception as e:
                logging.error(f"Giveaway join error: {e}")

    # Парсим старый giveaway формат
    giveaway_id = None
    actual_ref = ref_code
    if ref_code and '_giveaway_' in ref_code:
        parts = ref_code.split('_giveaway_')
        actual_ref = parts[0]
        giveaway_id = parts[1]

    webapp_url_giveaway = f"{WEBAPP_URL}#giveaway={giveaway_id}" if giveaway_id else WEBAPP_URL

    if giveaway_id:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎁 Участвовать в розыгрыше", web_app=WebAppInfo(url=webapp_url_giveaway))],
            [InlineKeyboardButton(text="🦄 Open Memstroy", web_app=WebAppInfo(url=WEBAPP_URL))],
        ])
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🦄 Open Memstroy", web_app=WebAppInfo(url=WEBAPP_URL)),
                InlineKeyboardButton(text="💬 Community", url="https://t.me/memstroy_community"),
            ],
            [
                InlineKeyboardButton(text="💬 Чат", url="https://t.me/memstroy_chat"),
                InlineKeyboardButton(text="🆘 Support", url="https://t.me/memstroy_support"),
            ]
        ])
    text = (
        f"Привет, {name}! 👋\n\n"
        f"🎴 <b>Memstroy</b> — коллекционные карточки в Telegram\n\n"
        f"Первая коллекция <b>Ponki</b> уже доступна:\n"
        f"🃏 50 уникальных моделей · 10 000 карточек\n"
        f"💎 Зарабатывай гемы за задания\n"
        f"📈 Торгуй на рынке с другими игроками\n"
        f"🎰 Испытай удачу в играх\n\n"
        f"👇 Открывай и начинай собирать!"
    )
    try:
        from aiogram.types import FSInputFile
        photo = FSInputFile("/root/memstroy/static/icons/ponki_and_pepe.png")
        await message.answer_photo(photo=photo, caption=text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await message.answer(text, reply_markup=kb, parse_mode="HTML")


@dp.message(Command("check"))
async def check_subscription(message: types.Message):
    """Admin: /check @username — проверить подписки, автозапись если все 3 ок"""
    if message.from_user.id != ADMIN_ID:
        return

    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /check @username или /check 123456789")
        return

    target = parts[1].lstrip('@')

    target_id = None
    target_username = target
    if target.isdigit():
        target_id = int(target)
    else:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(f"{API_URL}/api/check_user", json={"query": target}) as resp:
                    data = await resp.json(content_type=None)
                    target_id = data.get("telegram_id")
                    target_username = data.get("username") or target
            except:
                pass

    if not target_id:
        lines = [f"🔍 Проверка @{target}\n"]
        lines.append("❌ Канал @memstroy_community — нет данных (не в боте)")
        lines.append("❌ Чат @memstroy_chat — нет данных (не в боте)")
        lines.append("❌ Бот — не зарегистрирован в Memstroy")
        await message.answer("\n".join(lines))
        return

    lines = [f"🔍 Проверка @{target} (id: {target_id})\n"]

    all_ok = True
    for name, chat_id in [("Канал @memstroy_community", "@memstroy_community"),
                           ("Чат @memstroy_chat", "@memstroy_chat")]:
        try:
            member = await bot.get_chat_member(chat_id, target_id)
            if member.status in ("member", "administrator", "creator"):
                lines.append(f"✅ {name}")
            elif member.status == "kicked":
                lines.append(f"🚫 {name} — заблокирован")
                all_ok = False
            else:
                lines.append(f"❌ {name} — не подписан")
                all_ok = False
        except Exception as e:
            lines.append(f"⚠️ {name} — ошибка проверки")
            all_ok = False

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{API_URL}/api/user/{target_id}") as resp:
                if resp.status == 200:
                    udata = await resp.json(content_type=None)
                    gems = udata.get("gems", 0)
                    cards = len(udata.get("cards", []))
                    lines.append(f"✅ Бот — зарегистрирован (💎{gems} гемов, 🃏{cards} карт)")
                else:
                    lines.append(f"❌ Бот — не зарегистрирован")
                    all_ok = False
        except:
            lines.append(f"⚠️ Бот — ошибка проверки")
            all_ok = False

    # Проверяем — уже в списке медведей?
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{API_URL}/api/gift/check", json={
                "telegram_id": target_id, "gift_type": "bear"
            }) as resp:
                gdata = await resp.json(content_type=None)
        except:
            gdata = {}

    if gdata.get("already_given"):
        given_at = (gdata.get("given_at") or "")[:10]
        lines.append(f"\n🐻 Уже в списке с {given_at} — подарок выдан ранее")
        await message.answer("\n".join(lines), parse_mode="HTML")
        return

    if all_ok:
        async with aiohttp.ClientSession() as session:
            try:
                await session.post(f"{API_URL}/api/gift/mark", json={
                    "telegram_id": target_id,
                    "username": target_username,
                    "gift_type": "bear",
                    "given_by": ADMIN_ID
                })
            except:
                pass
        lines.append(f"\n✅ <b>Все условия выполнены!</b>")
        lines.append(f"🐻 Записан в список")
    else:
        lines.append(f"\n⛔ Не все условия — в список не записан")

    await message.answer("\n".join(lines), parse_mode="HTML")



@dp.message(Command("stats"))
async def stats(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{API_URL}/api/admin/stats") as resp:
                if resp.status != 200:
                    await message.answer(f"❌ API вернул {resp.status}: {await resp.text()}")
                    return
                d = await resp.json(content_type=None)
            text = (
                f"📊 <b>Статистика Memstroy</b>\n\n"
                f"👥 Всего юзеров: <b>{d.get('total_users', 0)}</b>\n"
                f"🆕 Новых сегодня: <b>{d.get('new_today', 0)}</b>\n"
                f"🎯 Активных сегодня: <b>{d.get('active_today', 0)}</b>\n\n"
                f"🃏 Карточек выдано: <b>{d.get('total_cards', 0)}</b>\n"
                f"⭐ Звёзд потрачено: <b>{d.get('total_stars', 0)}</b>\n"
                f"🛒 Куплено карт за ⭐: <b>{d.get('total_trades', 0)}</b>\n"
                f"📈 Сделок на рынке: <b>{d.get('market_trades', d.get('total_trades', 0))}</b>\n"
                f"💰 TON оборот: <b>{d.get('total_ton', 0)} TON</b>\n\n"
                f"💎 Гемов в системе: <b>{d.get('total_gems', 0)}</b>\n\n"
                f"🏆 Топ покупатель: <b>{d.get('top_buyer', '—')}</b>"
            )
            await message.answer(text, parse_mode="HTML")
        except Exception as e:
            await message.answer(f"❌ Ошибка /stats: {e}")


@dp.message(Command("stars"))
async def stars_cmd(message: types.Message):
    """Admin: /stars — кто кого пригласил по реф. программе Stars"""
    if message.from_user.id != ADMIN_ID:
        return
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{API_URL}/api/admin/stars_stats",
                                   params={"telegram_id": ADMIN_ID}) as resp:
                if resp.status != 200:
                    await message.answer(f"❌ API вернул {resp.status}: {await resp.text()}")
                    return
                d = await resp.json(content_type=None)

            recent = d.get("recent_invites", [])
            if not recent:
                await message.answer("⭐ <b>Stars — приглашения</b>\n\nПока пусто.", parse_mode="HTML")
                return

            lines = [f"⭐ <b>Stars — приглашения ({len(recent)})</b>\n"]
            for r in recent:
                ref = f"@{r['referrer']}" if r.get("referrer") else f"id{r['referrer_tg']}"
                inv = f"@{r['invited']}" if r.get("invited") else f"id{r['invited_tg']}"
                status = "✅" if r["status"] == "done" else ("❌" if r["status"] == "failed" else "⏳")
                date = r.get("joined_at", "")[:10]
                lines.append(f"{status} {ref} → {inv} | {date}")

            await message.answer("\n".join(lines), parse_mode="HTML")
        except Exception as e:
            await message.answer(f"❌ Ошибка /stars: {e}")


@dp.message(Command("stars_list"))
async def stars_list_cmd(message: types.Message):
    """Admin: /stars_list — активные заявки на вывод"""
    if message.from_user.id != ADMIN_ID:
        return
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{API_URL}/api/admin/withdraw_requests",
                               params={"telegram_id": ADMIN_ID}) as resp:
            data = await resp.json(content_type=None)

    pending = [r for r in data.get("requests", []) if r["status"] == "pending"]
    if not pending:
        await message.answer("✅ Нет активных заявок на вывод")
        return

    lines = [f"💫 <b>Заявки на вывод ({len(pending)} шт):</b>\n"]
    for r in pending:
        uname = f"@{r['username']}" if r.get('username') else str(r['telegram_id'])
        lines.append(f"👤 {uname}\n⭐ {r['amount']} Stars\n✅ /stars_done {r['telegram_id']}\n")
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(lambda m: m.text and m.text.startswith("/stars_done") and m.from_user.id == ADMIN_ID)
async def stars_done_cmd(message: types.Message):
    """Admin: /stars_done <telegram_id> — подтвердить вывод"""
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.answer("❌ Использование: /stars_done <telegram_id>")
        return
    try:
        target_tg = int(parts[1])
    except ValueError:
        await message.answer("❌ Неверный telegram_id")
        return

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{API_URL}/api/admin/withdraw_requests",
                               params={"telegram_id": ADMIN_ID}) as resp:
            data = await resp.json(content_type=None)

        pending = next((r for r in data.get("requests", [])
                        if r["telegram_id"] == target_tg and r["status"] == "pending"), None)
        if not pending:
            await message.answer(f"❌ Активной заявки для {target_tg} не найдено")
            return

        async with session.post(f"{API_URL}/api/admin/withdraw_done",
                                json={"telegram_id": ADMIN_ID, "withdraw_id": pending["id"]}) as resp2:
            result = await resp2.json(content_type=None)

    if result.get("ok"):
        uname = f"@{pending['username']}" if pending.get('username') else str(target_tg)
        await message.answer(
            f"✅ Готово!\n👤 {uname}\n⭐ {pending['amount']} Stars отправлено\nЮзер получил уведомление."
        )
    else:
        await message.answer(f"❌ Ошибка: {result}")


async def send_daily_reminder():
    """Send daily reminder to all users at 08:30 UTC"""
    import os
    api_url = os.getenv("API_URL", "http://localhost:8000")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{api_url}/api/all_users") as resp:
                users = await resp.json()
        for user in users:
            tg_id = user.get("telegram_id")
            if not tg_id:
                continue
            try:
                await bot.send_message(
                    tg_id,
                    "☀️ Доброго дня!\n\n🎁 Твоя ежедневная награда ждёт тебя!\nЗаходи в Memstroy и забирай гемы 💎\n\nЧем дольше стрик — тем больше награда 🔥",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="🦄 Забрать награду", web_app=WebAppInfo(url=os.getenv("WEBAPP_URL","https://memstroy.app")))
                    ]])
                )
                await asyncio.sleep(0.05)
            except Exception:
                pass
    except Exception as e:
        logging.error(f"Daily reminder error: {e}")


async def scheduler():
    from datetime import datetime, timezone
    while True:
        now = datetime.now(timezone.utc)
        target = now.replace(hour=8, minute=30, second=0, microsecond=0)
        if now >= target:
            target = target.replace(day=target.day+1)
        wait_seconds = (target - now).total_seconds()
        await asyncio.sleep(wait_seconds)
        await send_daily_reminder()


async def main():
    asyncio.create_task(scheduler())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
