import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, LabeledPrice, PreCheckoutQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://yourdomain.com")

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


@dp.message(CommandStart())
async def start(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Open",
            web_app=WebAppInfo(url=WEBAPP_URL)
        )]
    ])
    
    # Pass referral if exists
    ref_code = None
    if message.text and len(message.text.split()) > 1:
        ref_code = message.text.split()[1]
    
    name = message.from_user.username or message.from_user.first_name or "friend"
    await message.answer(
        f"Hey, @{name} 👋",
        reply_markup=kb
    )
    
    # Register user via API
    import aiohttp
    async with aiohttp.ClientSession() as session:
        user = message.from_user
        payload = {
            "telegram_id": user.id,
            "username": user.username or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "ref_code": ref_code
        }
        try:
            async with session.post(f"http://localhost:8000/api/register", json=payload) as resp:
                pass
        except:
            pass


@dp.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(query.id, ok=True)


@dp.message(F.successful_payment)
async def successful_payment(message: types.Message):
    payload = message.successful_payment.invoice_payload
    # payload format: "buy_stars_{user_id}_{amount}" or "upgrade_{user_id}_{card_id}" etc.
    import aiohttp
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"http://localhost:8000/api/payment/confirm", json={
                "telegram_id": message.from_user.id,
                "payload": payload,
                "stars": message.successful_payment.total_amount
            }) as resp:
                data = await resp.json()
                await message.answer(f"✅ {data.get('message', 'Payment confirmed!')}")
        except Exception as e:
            await message.answer("✅ Payment received!")


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
