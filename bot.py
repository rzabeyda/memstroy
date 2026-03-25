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

    # Парсим giveaway из start_param
    giveaway_id = None
    actual_ref = ref_code
    if ref_code and '_giveaway_' in ref_code:
        parts = ref_code.split('_giveaway_')
        actual_ref = parts[0]
        giveaway_id = parts[1]

    # URL с giveaway через hash — читается фронтом
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


@dp.message(Command("stats"))
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



async def web_app_data_handler(message: types.Message):
    """Handle buy requests from Mini App via sendData"""
    try:
        data = json.loads(message.web_app_data.data)
        if data.get("action") == "buy_stars":
            collection_id = data.get("collection_id", 1)
            qty = max(1, int(data.get("qty", 1)))
            price_per = 1  # 1 star per card for testing
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

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{API_URL}/api/payment/confirm", json={
                "telegram_id": message.from_user.id,
                "payload": payload,
                "stars": stars_paid
            }) as resp:
                data = await resp.json()
                # Silent - user sees result in app
                pass
        except Exception as e:
            logging.error(f"payment confirm error: {e}")
            await message.answer(
                "✅ Payment received!",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Open", web_app=WebAppInfo(url=WEBAPP_URL))]
                ])
            )


async def send_daily_reminder():
    """Send daily reminder to all users at 08:30 UTC (11:30 MSK)"""
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
                await asyncio.sleep(0.05)  # avoid flood
            except Exception:
                pass
    except Exception as e:
        logging.error(f"Daily reminder error: {e}")


async def scheduler():
    """Run daily reminder at 08:30 UTC every day"""
    from datetime import datetime, timezone
    while True:
        now = datetime.now(timezone.utc)
        # Next 08:30 UTC
        target = now.replace(hour=8, minute=30, second=0, microsecond=0)
        if now >= target:
            target = target.replace(day=target.day+1)
        wait_seconds = (target - now).total_seconds()
        logging.info(f"Next daily reminder in {wait_seconds/3600:.1f} hours")
        await asyncio.sleep(wait_seconds)
        await send_daily_reminder()


async def main():
    asyncio.create_task(scheduler())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
