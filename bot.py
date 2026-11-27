"""
Userbot-автоответчик на Pyrogram.

Каждому, кто впервые пишет в личку:
  1) через 1–2 минуты "читает" сообщение, "печатает" и отвечает, что вакансия уже не актуальна;
  2) ещё примерно через 5 минут снова "печатает" и по-дружески советует заглянуть в канал.
Каждому пользователю отвечает только один раз (ID хранятся в SQLite-базе users.db).
Всё асинхронно: каждый диалог идёт своей задачей, бот ведёт сколько угодно людей одновременно.
"""

import asyncio
import logging
import os
import random

import aiosqlite
from dotenv import load_dotenv
from pyrogram import Client, enums, filters, idle
from pyrogram.types import Message

load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_NAME = os.getenv("SESSION_NAME", "my_account")
# Строка сессии (получить: python login.py). Если задана, файл .session не нужен
SESSION_STRING = os.getenv("SESSION_STRING", "").strip() or None
WORKDIR = os.getenv("WORKDIR", ".")
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "https://t.me/+fpeV0o0QOIo4MTli")
DB_FILE = os.getenv("DB_FILE", "users.db")

os.makedirs(os.path.dirname(os.path.abspath(DB_FILE)), exist_ok=True)

# Задержки в секундах
FIRST_DELAY = (60, 120)      # перед первым ответом: 1–2 минуты
SECOND_DELAY = (270, 330)    # между первым и вторым сообщением: ~5 минут

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("userbot")

# ---------------------------------------------------------------------------
# Шаблоны сообщений. Каждый раз выбирается случайный, чтобы не было одинаково.
# ---------------------------------------------------------------------------

FIRST_MESSAGES = [
    "Здравствуйте! К сожалению, вакансия уже не актуальна, мы нашли человека",
    "Добрый день) Вакансия уже закрыта, извините",
    "Здравствуйте, спасибо, что откликнулись! Но место уже занято, вакансия не актуальна",
    "Привет! Увы, уже не актуально, взяли человека на прошлой неделе",
    "Здравствуйте. Вакансия, к сожалению, уже неактуальна",
    "Добрый день! Спасибо за интерес, но вакансию уже закрыли",
    "Здравствуйте) Уже не актуально, к сожалению, извините что так",
]

SECOND_MESSAGES = [
    "Если что, можете посмотреть вот тут, там часто выкладывают похожие вакансии {link}",
    "Кстати, если ещё ищете, гляньте этот канал, мы сами там людей ищем {link}",
    "Если есть желание, можно поискать здесь, там регулярно что-то новое появляется {link}",
    "Если вдруг актуально, попробуйте поискать тут {link}\nМожет что-то подходящее найдётся",
    "Могу подсказать канал, там бывают похожие предложения, может пригодится {link}",
    "Если ещё в поиске, посмотрите вот этот канал {link}\nТам довольно много вариантов",
    "Кстати, вот тут периодически публикуют вакансии, можете глянуть {link}",
]

# ---------------------------------------------------------------------------
# База данных (SQLite через aiosqlite): кому уже ответили и на каком этапе
#   stage = 0  — человек написал, ответ ещё не отправлен
#   stage = 1  — отправлено "вакансия не актуальна"
#   stage = 2  — отправлен совет с каналом, диалог завершён
# ---------------------------------------------------------------------------


class Storage:
    def __init__(self, path: str):
        self.path = path
        self.db: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self.db = await aiosqlite.connect(self.path)
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS users (
                   user_id    INTEGER PRIMARY KEY,
                   chat_id    INTEGER NOT NULL,
                   name       TEXT,
                   stage      INTEGER NOT NULL DEFAULT 0,
                   created_at TEXT NOT NULL DEFAULT (datetime('now')),
                   updated_at TEXT NOT NULL DEFAULT (datetime('now'))
               )"""
        )
        await self.db.commit()

    async def add_if_new(self, user_id: int, chat_id: int, name: str) -> bool:
        """Добавляет пользователя. True — если он новый (ему ещё не отвечали)."""
        cur = await self.db.execute(
            "INSERT OR IGNORE INTO users (user_id, chat_id, name) VALUES (?, ?, ?)",
            (user_id, chat_id, name),
        )
        await self.db.commit()
        return cur.rowcount == 1

    async def set_stage(self, user_id: int, stage: int) -> None:
        await self.db.execute(
            "UPDATE users SET stage = ?, updated_at = datetime('now') WHERE user_id = ?",
            (stage, user_id),
        )
        await self.db.commit()

    async def unfinished(self) -> list[tuple[int, int, str, int]]:
        cur = await self.db.execute(
            "SELECT user_id, chat_id, name, stage FROM users WHERE stage < 2"
        )
        return await cur.fetchall()

    async def count(self) -> int:
        cur = await self.db.execute("SELECT COUNT(*) FROM users")
        return (await cur.fetchone())[0]

    async def close(self) -> None:
        if self.db:
            await self.db.close()


storage = Storage(DB_FILE)
tasks: set[asyncio.Task] = set()  # держим ссылки, чтобы задачи не собрал сборщик мусора


def start_dialog(client: Client, user_id: int, chat_id: int, name: str, stage: int = 0) -> None:
    """Каждый диалог — отдельная asyncio-задача: паузы одного не мешают другим."""
    task = asyncio.create_task(handle_user(client, user_id, chat_id, name, stage))
    tasks.add(task)
    task.add_done_callback(tasks.discard)


# ---------------------------------------------------------------------------
# Имитация человека
# ---------------------------------------------------------------------------


def typing_duration(text: str) -> float:
    """Сколько секунд "печатать": примерно 4–7 символов в секунду, с паузами."""
    speed = random.uniform(4, 7)
    seconds = len(text) / speed + random.uniform(1, 3)
    return max(3.0, min(seconds, 25.0))


async def type_and_send(client: Client, chat_id: int, text: str) -> None:
    duration = typing_duration(text)
    elapsed = 0.0
    # Статус "печатает..." держится ~5 секунд, поэтому обновляем его каждые 4 с
    while elapsed < duration:
        await client.send_chat_action(chat_id, enums.ChatAction.TYPING)
        step = min(4.0, duration - elapsed)
        await asyncio.sleep(step)
        elapsed += step
    await client.send_message(chat_id, text, disable_web_page_preview=False)


async def handle_user(client: Client, user_id: int, chat_id: int, name: str, stage: int = 0) -> None:
    try:
        if stage < 1:
            # 1. Ждём 1–2 минуты, будто не сразу увидели сообщение
            await asyncio.sleep(random.uniform(*FIRST_DELAY))

            # 2. "Читаем" сообщение и немного думаем
            await client.read_chat_history(chat_id)
            await asyncio.sleep(random.uniform(2, 6))

            # 3. Печатаем и отвечаем, что вакансия не актуальна
            await type_and_send(client, chat_id, random.choice(FIRST_MESSAGES))
            await storage.set_stage(user_id, 1)
            log.info("Первое сообщение отправлено: %s (%s)", name, user_id)

            # 4. Ждём ~5 минут
            await asyncio.sleep(random.uniform(*SECOND_DELAY))
        else:
            # Продолжаем после перезапуска бота: небольшая пауза и сразу совет
            await asyncio.sleep(random.uniform(30, 90))

        # 5. Печатаем совет со ссылкой на канал
        await client.read_chat_history(chat_id)
        text = random.choice(SECOND_MESSAGES).format(link=CHANNEL_LINK)
        await type_and_send(client, chat_id, text)
        await storage.set_stage(user_id, 2)
        log.info("Второе сообщение отправлено: %s (%s)", name, user_id)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Ошибка при ответе пользователю %s", user_id)


# ---------------------------------------------------------------------------
# Клиент
# ---------------------------------------------------------------------------

app = Client(
    SESSION_NAME,
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    workdir=WORKDIR,
)


@app.on_message(filters.private & filters.incoming & ~filters.me & ~filters.bot & ~filters.service)
async def on_private_message(client: Client, message: Message) -> None:
    user = message.from_user
    if user is None or user.is_self or user.is_bot or user.is_support:
        return
    name = user.username or user.first_name or str(user.id)
    # INSERT OR IGNORE атомарно: даже если человек прислал 5 сообщений подряд,
    # диалог запустится только один раз
    if not await storage.add_if_new(user.id, message.chat.id, name):
        return

    log.info("Новый собеседник: %s (%s), отвечу позже", name, user.id)
    start_dialog(client, user.id, message.chat.id, name)


async def main() -> None:
    if not API_ID or not API_HASH:
        raise SystemExit("Заполните API_ID и API_HASH в файле .env")

    await storage.connect()
    await app.start()
    log.info("Бот запущен. В базе %d пользователей.", await storage.count())

    # Досылаем то, что не успели до прошлой остановки
    for user_id, chat_id, name, stage in await storage.unfinished():
        log.info("Продолжаю незавершённый диалог: %s (этап %d)", name, stage)
        start_dialog(app, user_id, chat_id, name, stage)

    try:
        await idle()
    finally:
        for t in tasks:
            t.cancel()
        await app.stop()
        await storage.close()


if __name__ == "__main__":
    app.run(main())
