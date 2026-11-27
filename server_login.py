"""
Вход в Telegram на сервере без интерактивного ввода, в два шага.

  1) python server_login.py send +79991234567
     Telegram пришлёт код в приложение.
  2) python server_login.py code 12345 [пароль_2FA]
     Создаётся файл сессии (SESSION_NAME.session в WORKDIR), после этого
     можно запускать bot.py.

API_ID, API_HASH, SESSION_NAME и WORKDIR берутся из .env.
"""
import asyncio
import json
import os
import sys

from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import SessionPasswordNeeded

load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_NAME = os.getenv("SESSION_NAME", "my_account")
WORKDIR = os.getenv("WORKDIR", ".")
STATE = os.path.join(WORKDIR, ".login_state.json")


def client() -> Client:
    os.makedirs(WORKDIR, exist_ok=True)
    return Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH, workdir=WORKDIR)


async def send(phone: str) -> None:
    app = client()
    await app.connect()
    sent = await app.send_code(phone)
    await app.disconnect()
    with open(STATE, "w") as f:
        json.dump({"phone": phone, "hash": sent.phone_code_hash}, f)
    print(f"Код отправлен ({sent.type.name}). Теперь: python server_login.py code <код>")


async def code(value: str, password: str | None) -> None:
    with open(STATE) as f:
        st = json.load(f)
    app = client()
    await app.connect()
    try:
        await app.sign_in(st["phone"], st["hash"], value)
    except SessionPasswordNeeded:
        if not password:
            print("Включена двухфакторная защита: python server_login.py code <код> <пароль>")
            await app.disconnect()
            return
        await app.check_password(password)
    me = await app.get_me()
    await app.disconnect()
    os.remove(STATE)
    print(f"Готово, вход выполнен как {me.first_name} (@{me.username}). Можно запускать бота.")


if __name__ == "__main__":
    if not API_ID or not API_HASH:
        sys.exit("Заполните API_ID и API_HASH в .env")
    if len(sys.argv) >= 3 and sys.argv[1] == "send":
        asyncio.run(send(sys.argv[2]))
    elif len(sys.argv) >= 3 and sys.argv[1] == "code":
        asyncio.run(code(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None))
    else:
        sys.exit(__doc__)
