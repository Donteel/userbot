"""
Запустите ОДИН РАЗ у себя на компьютере: python login.py
Войдёте по номеру телефона и коду, скрипт выведет SESSION_STRING.
Её нужно вставить в .env на сервере, тогда вводить код на сервере не придётся.
"""
import asyncio
import os

from dotenv import load_dotenv
from pyrogram import Client

load_dotenv()


async def main() -> None:
    api_id = int(os.getenv("API_ID") or input("API_ID: "))
    api_hash = os.getenv("API_HASH") or input("API_HASH: ")
    async with Client("login_tmp", api_id=api_id, api_hash=api_hash, in_memory=True) as app:
        me = await app.get_me()
        session = await app.export_session_string()
    print(f"\nВход выполнен как {me.first_name} (@{me.username})")
    print("\nДобавьте эту строку в .env на сервере (никому её не показывайте!):\n")
    print(f"SESSION_STRING={session}\n")


if __name__ == "__main__":
    asyncio.run(main())
