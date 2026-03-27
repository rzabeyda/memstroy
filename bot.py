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



async def stats(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{API_URL}/api/admin/stats") as resp:
                d = await resp.json(content_type=None)
            text = (
                f"📊 <b>Статистика Memstroy</b>\n\n"
                f"👥 Всего юзеров: <b>{d.get('total_users', 0)}</b>\n"
                f"🆕 Новых сегодня: <b>{d.get('new_today', 0)}</b>\n"
                f"🎯 Активных сегодня: <b>{d.get('active_today', 0)}</b>\n\n"
                f"🃏 Карточек выдано: <b>{d.get('total_cards', 0)}</b>\n"
                f"⭐ Звёзд потрачено: <b>{d.get('total_stars', 0)}</b>\n"
                f"📈 Сделок на рынке: <b>{d.get('total_trades', 0)}</b>\n"
                f"💰 TON оборот: <b>{d.get('total_ton', 0)} TON</b>\n\n"
                f"💎 Гемов в системе: <b>{d.get('total_gems', 0)}</b>\n"
                f"✅ Заданий выполнено: <b>{d.get('tasks_done', 0)}</b>\n\n"
                f"🏆 Топ покупатель: <b>{d.get('top_buyer', '—')}</b>"
            )
            await message.answer(text, parse_mode="HTML")
        except Exception as e:
            await message.answer(f"Ошибка: {e}")


@dp.message(Command("pickwinner"))
async def pickwinner_cmd(message: types.Message):
    """Admin: /pickwinner giveaway1 — выбрать победителей"""
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    giveaway_key = parts[1] if len(parts) > 1 else "giveaway1"

    await message.answer(f"⏳ Выбираю победителей розыгрыша <b>{giveaway_key}</b>...", parse_mode="HTML")

    # card_ids для giveaway1
    GIVEAWAY_CARDS = {
        "giveaway1": [210,211,212,213,214,215,216,217,218,219,220]
    }
    card_ids = GIVEAWAY_CARDS.get(giveaway_key, [])

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{API_URL}/api/simple_giveaway/pick", json={
                "telegram_id": ADMIN_ID,
                "giveaway_key": giveaway_key,
                "card_ids": card_ids,
                "winners_count": len(card_ids)
            }) as resp:
                data = await resp.json(content_type=None)

            winners = data.get("winners", [])
            total = data.get("total_participants", 0)

            # Формируем список победителей
            winners_text = ""
            for i, w in enumerate(winners, 1):
                name = w.get("first_name") or w.get("username") or "Участник"
                username = f"@{w['username']}" if w.get("username") else f"id{w['telegram_id']}"
                card = w.get("card", "карточка")
                winners_text += f"{i}. {username} — {card}\n"

            result_text = (
                f"🏆 <b>Итоги розыгрыша {giveaway_key}!</b>\n\n"
                f"👥 Всего участников: <b>{total}</b>\n"
                f"🎁 Победителей: <b>{len(winners)}</b>\n\n"
                f"<b>Победители:</b>\n{winners_text}\n"
                f"✅ Карточки уже начислены, уведомления отправлены!"
            )

            # Постим в канал
            await session.post(
                f"https://api.telegram.org/bot{os.getenv('BOT_TOKEN')}/sendMessage",
                json={
                    "chat_id": "@memstroy_community",
                    "text": result_text,
                    "parse_mode": "HTML"
                }
            )

            await message.answer(result_text, parse_mode="HTML")

        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}")


@dp.message(Command("giveaway"))
async def giveaway_cmd(message: types.Message):
    """Admin command: /giveaway 10 24 — раздать 10 карточек через 24 часа"""
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) < 3:
        await message.answer(
            "📋 Использование:\n"
            "<code>/giveaway [кол-во карт] [часов]</code>\n\n"
            "Пример: <code>/giveaway 10 24</code>\n"
            "→ Розыгрыш 10 карточек через 24 часа\n\n"
            "Участники должны быть:\n"
            "• Подписаны на @memstroy_community\n"
            "• Зарегистрированы в боте",
            parse_mode="HTML"
        )
        return

    try:
        count = int(parts[1])
        hours = int(parts[2])
    except:
        await message.answer("❌ Неверный формат. Пример: /giveaway 10 24")
        return

    if count < 1 or count > 50:
        await message.answer("❌ Количество карточек: от 1 до 50")
        return
    if hours < 1 or hours > 168:
        await message.answer("❌ Время: от 1 до 168 часов (7 дней)")
        return

    await message.answer("⏳ Создаю розыгрыш...")

    async with aiohttp.ClientSession() as session:
        try:
            payload = {
                "telegram_id": ADMIN_ID,
                "winners_count": count,
                "hours": hours,
                "channel_username": "memstroy_community",
                "filter_type": "channel"
            }
            async with session.post(f"{API_URL}/api/giveaway/quick", json=payload) as resp:
                data = await resp.json(content_type=None)

            if data.get("ok"):
                gid = data.get("giveaway_id")
                await message.answer(
                    f"✅ <b>Розыгрыш создан!</b>\n\n"
                    f"🎁 Карточек: <b>{count}</b>\n"
                    f"⏰ Завершится через: <b>{hours} ч.</b>\n"
                    f"👥 Нужно подписаться на @memstroy_community\n\n"
                    f"ID розыгрыша: <code>{gid}</code>\n\n"
                    f"Анонс отправлен в канал автоматически!",
                    parse_mode="HTML"
                )
            else:
                await message.answer(f"❌ Ошибка: {data.get('detail', 'неизвестно')}")
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}")


@dp.message(Command("bears"))
async def bears_list_cmd(message: types.Message):
    """/bears — список всех кто получил мишку"""
    if message.from_user.id != ADMIN_ID:
        return
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{API_URL}/api/gift/list") as resp:
                gifts = await resp.json(content_type=None)
        except:
            gifts = []

    bears = [g for g in gifts if g.get("gift_type") == "bear"]
    if not bears:
        await message.answer("🐻 Список пуст — мишек ещё не выдавали")
        return

    lines = [f"🐻 <b>Получили мишку ({len(bears)}):</b>\n"]
    for i, g in enumerate(bears, 1):
        username = f"@{g['username']}" if g.get('username') else f"id{g['telegram_id']}"
        date = (g.get('given_at') or '')[:10]
        lines.append(f"{i}. {username} — {date}")

    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("winner"))
async def pick_winners(message: types.Message):
    """Admin: /winner giveaway1 — выбрать победителей"""
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /winner giveaway1")
        return
    giveaway_key = parts[1]
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{API_URL}/api/simple_giveaway/pick", json={
                "telegram_id": ADMIN_ID,
                "giveaway_key": giveaway_key,
                "card_ids": [210,211,212,213,214,215,216,217,218,219,220]
            }) as resp:
                data = await resp.json(content_type=None)
            winners = data.get("winners", [])
            text = f"🏆 <b>Победители розыгрыша!</b>\n\n"
            for i, w in enumerate(winners, 1):
                name = w.get("name","")
                username = f"@{w['username']}" if w.get("username") else name
                card = w.get("card","карточка")
                text += f"{i}. {username} — {card}\n"
            text += f"\n🎉 Карточки отправлены победителям!"
            await message.answer(text, parse_mode="HTML")
            # Post to channel
            async with session.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": "@memstroy_community", "text": text, "parse_mode": "HTML"}
            ) as r:
                pass
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}")



async def end_giveaway_cmd(message: types.Message):
    """Admin: /endgiveaway [id] — завершить розыгрыш досрочно"""
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /endgiveaway [id]")
        return
    gid = parts[1]
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{API_URL}/api/giveaway/finish", json={"giveaway_id": int(gid), "telegram_id": ADMIN_ID}) as resp:
                data = await resp.json(content_type=None)
            winners = data.get("winners", [])
            await message.answer(
                f"🏆 Розыгрыш завершён!\n"
                f"Победителей: {len(winners)}\n"
                f"Уведомления отправлены ✅"
            )
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}")


async def web_app_data_handler(message: types.Message):
    """Handle buy requests from Mini App via sendData"""
    try:
        data = json.loads(message.web_app_data.data)
        if data.get("action") == "buy_stars":
            collection_id = data.get("collection_id", 1)
            qty = max(1, int(data.get("qty", 1)))
            price_per = 1
            prices = [LabeledPrice(
                label=f"Ponki Cards x{qty}",
                amount=price_per * qty
            )]
            await bot.send_invoice(
                chat_id=message.from_user.id,
                title="Ponki Card Pack",
                description=f"Open {qty} Ponki card pack{'s' if qty > 1 else ''}. Each card is unique!",
                payload=f"buy_card_{collection_id}_{qty}",
                currency="XTR",
                prices=prices,
            )
    except Exception as e:
        logging.error(f"web_app_data error: {e}")


@dp.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(query.id, ok=True)


@dp.message(F.successful_payment)
async def successful_payment(message: types.Message):
    payload = message.successful_payment.invoice_payload
    stars_paid = message.successful_payment.total_amount
    telegram_id = message.from_user.id

    # GEM pool payment
    if payload.startswith("gem_pool_"):
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(f"{API_URL}/api/gem/confirm", json={
                    "telegram_id": telegram_id,
                    "payload": payload,
                    "stars": stars_paid
                }) as resp:
                    data = await resp.json()

                if data.get("draw"):
                    if data.get("i_won"):
                        await message.answer("🎉 Вы выиграли GEM! Подарок будет отправлен в ближайшее время ✨")
                    else:
                        winner = data.get("winner", "?")
                        await message.answer(f"😔 Не повезло. Победил @{winner}. Попробуйте ещё раз!")
                else:
                    tickets = data.get("total_tickets", 0)
                    left = 5 - tickets
                    await message.answer(f"✅ Билет куплен! В пуле {tickets}/5 билетов. Ждём ещё {left}...")
            except Exception as e:
                logging.error(f"gem confirm error: {e}")
                await message.answer("✅ Оплата получена!")
        return

    # Regular payment
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{API_URL}/api/payment/confirm", json={
                "telegram_id": telegram_id,
                "payload": payload,
                "stars": stars_paid
            }) as resp:
                data = await resp.json()
        except Exception as e:
            logging.error(f"payment confirm error: {e}")
            await message.answer(
                "✅ Payment received!",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Open", web_app=WebAppInfo(url=WEBAPP_URL))]
                ])
            )


async def send_daily_reminder():
    """Send daily reminder to all users at 08:30 UTC"""
    import aiohttp, os
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
