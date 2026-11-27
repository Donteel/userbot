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
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiosqlite
from dotenv import load_dotenv
from pyrogram import Client, enums, filters, idle
from pyrogram.errors import ChatAdminRequired, UserNotParticipant
from pyrogram.types import Chat, Message

load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_NAME = os.getenv("SESSION_NAME", "my_account")
# Строка сессии (получить: python login.py). Если задана, файл .session не нужен
SESSION_STRING = os.getenv("SESSION_STRING", "").strip() or None
WORKDIR = os.getenv("WORKDIR", ".")
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "https://t.me/+fpeV0o0QOIo4MTli")
# Часовой пояс для статистики «за сегодня»
TZ = ZoneInfo(os.getenv("TZ_NAME", "Asia/Jerusalem"))
STARTED_AT = time.time()
DB_FILE = os.getenv("DB_FILE", "users.db")
IGNORE_FILE = os.getenv("IGNORE_FILE", "ignore_list.txt")

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
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS ignored (
                   id         INTEGER PRIMARY KEY AUTOINCREMENT,
                   user_id    INTEGER UNIQUE,
                   username   TEXT UNIQUE COLLATE NOCASE,
                   added_at   TEXT NOT NULL DEFAULT (datetime('now'))
               )"""
        )
        await self.db.execute(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)"
        )
        await self.db.commit()
        await self._import_existing_once()
        await self._import_ignore_file()

    async def _import_ignore_file(self) -> None:
        """Добавляет username из ignore_list.txt (повторные запуски ничего не дублируют)."""
        if not os.path.exists(IGNORE_FILE):
            return
        added = 0
        with open(IGNORE_FILE, encoding="utf-8") as f:
            for line in f:
                name = line.strip().lstrip("@")
                if not name or name.startswith("#"):
                    continue
                cur = await self.db.execute(
                    "INSERT OR IGNORE INTO ignored (username) VALUES (?)", (name.lower(),)
                )
                added += cur.rowcount
        await self.db.commit()
        if added:
            log.info("Из %s добавлено в «не отвечать»: %d", IGNORE_FILE, added)

    async def _import_existing_once(self) -> None:
        """Один раз переносит всех, кто уже есть в базе, в список «не отвечать»."""
        cur = await self.db.execute("SELECT value FROM meta WHERE key = 'imported_existing'")
        if await cur.fetchone():
            return
        cur = await self.db.execute(
            "INSERT OR IGNORE INTO ignored (user_id, username) "
            "SELECT user_id, NULL FROM users"
        )
        await self.db.execute(
            "INSERT INTO meta (key, value) VALUES ('imported_existing', datetime('now'))"
        )
        await self.db.commit()
        log.info("Перенёс %d пользователей из базы в список «не отвечать»", cur.rowcount)

    # ---- список «не отвечать» ----

    async def is_ignored(self, user_id: int, username: str | None) -> bool:
        cur = await self.db.execute(
            "SELECT 1 FROM ignored WHERE user_id = ? OR (username IS NOT NULL AND username = ?)",
            (user_id, (username or "").lower() or None),
        )
        return await cur.fetchone() is not None

    async def add_ignored(self, user_id: int | None, username: str | None) -> bool:
        username = username.lower() if username else None
        # Если уже есть запись только по username или только по id — дополняем её
        cur = await self.db.execute(
            "SELECT id FROM ignored WHERE (user_id IS NOT NULL AND user_id = ?) "
            "OR (username IS NOT NULL AND username = ?)",
            (user_id, username),
        )
        row = await cur.fetchone()
        if row:
            await self.db.execute(
                "UPDATE ignored SET user_id = COALESCE(user_id, ?), "
                "username = COALESCE(username, ?) WHERE id = ?",
                (user_id, username, row[0]),
            )
            await self.db.commit()
            return False
        await self.db.execute(
            "INSERT INTO ignored (user_id, username) VALUES (?, ?)", (user_id, username)
        )
        await self.db.commit()
        return True

    async def remove_ignored(self, user_id: int | None, username: str | None) -> bool:
        cur = await self.db.execute(
            "DELETE FROM ignored WHERE (user_id IS NOT NULL AND user_id = ?) "
            "OR (username IS NOT NULL AND username = ?)",
            (user_id, (username or "").lower() or None),
        )
        await self.db.commit()
        return cur.rowcount > 0

    async def list_ignored(self) -> list[tuple[int | None, str | None, str]]:
        cur = await self.db.execute(
            "SELECT i.user_id, COALESCE(i.username, u.name), i.added_at "
            "FROM ignored i LEFT JOIN users u ON u.user_id = i.user_id ORDER BY i.added_at"
        )
        return await cur.fetchall()

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

    async def stats(self, since_utc: str, until_utc: str | None = None) -> dict:
        until_utc = until_utc or "9999-12-31"
        cur = await self.db.execute(
            "SELECT COUNT(*), SUM(stage >= 1), SUM(stage >= 2) FROM users "
            "WHERE created_at >= ? AND created_at < ?",
            (since_utc, until_utc),
        )
        total, first, second = await cur.fetchone()
        return {"total": total or 0, "first": first or 0, "second": second or 0}

    async def users_since(self, since_utc: str) -> list[tuple[int, str]]:
        cur = await self.db.execute(
            "SELECT user_id, name FROM users WHERE created_at >= ?", (since_utc,)
        )
        return await cur.fetchall()

    async def count_ignored(self) -> int:
        cur = await self.db.execute("SELECT COUNT(*) FROM ignored")
        return (await cur.fetchone())[0]

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


async def still_allowed(user_id: int, username: str | None) -> bool:
    if await storage.is_ignored(user_id, username):
        log.info("Пользователь %s в списке «не отвечать», диалог остановлен", user_id)
        return False
    return True


async def handle_user(client: Client, user_id: int, chat_id: int, name: str, stage: int = 0) -> None:
    try:
        if stage < 1:
            # 1. Ждём 1–2 минуты, будто не сразу увидели сообщение
            await asyncio.sleep(random.uniform(*FIRST_DELAY))

            # 2. "Читаем" сообщение и немного думаем
            await client.read_chat_history(chat_id)
            await asyncio.sleep(random.uniform(2, 6))
            if not await still_allowed(user_id, name):
                return

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
        if not await still_allowed(user_id, name):
            return
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
    if await storage.is_ignored(user.id, user.username):
        return
    name = user.username or user.first_name or str(user.id)
    # INSERT OR IGNORE атомарно: даже если человек прислал 5 сообщений подряд,
    # диалог запустится только один раз
    if not await storage.add_if_new(user.id, message.chat.id, name):
        return

    log.info("Новый собеседник: %s (%s), отвечу позже", name, user.id)
    start_dialog(client, user.id, message.chat.id, name)


# ---------------------------------------------------------------------------
# Команды управления. Пишите их себе в «Избранное» (Saved Messages):
#   /ignore            — бот спросит username, кому не отвечать
#   /ignore @user ...  — сразу добавить одного или нескольких
#   /unignore @user    — убрать из списка
#   /ignorelist        — показать список
#   /cancel            — отменить ожидание username
# Ещё можно написать /ignore прямо в чате с человеком — он попадёт в список,
# а сама команда удалится.
# ---------------------------------------------------------------------------

HELP_TEXT = (
    "🤖 Автоответчик\n\n"
    "Что делает: каждому, кто впервые пишет в личку, через 1–2 мин отвечает, "
    "что вакансия уже не актуальна, а ещё через ~5 мин советует канал. "
    "Каждому только один раз, с «печатает…». Люди из списка «не отвечать» "
    "и боты игнорируются.\n\n"
    "Команды (пишите сюда, в Избранное):\n"
    "/help — эта справка\n"
    "/test — проверить, что бот работает\n"
    "/stats — статистика: сколько написали сегодня, за неделю и всего, "
    "и сколько из них уже в канале\n"
    "/ignore — добавить в «не отвечать» (спрошу username)\n"
    "/ignore @user1 @user2 — добавить сразу\n"
    "/unignore @user — убрать из списка\n"
    "/ignorelist — показать список\n"
    "/cancel — отменить ввод\n\n"
    "Ещё можно написать /ignore прямо в чате с человеком — он попадёт в список, "
    "а команда удалится."
)

awaiting: dict[str, str] = {}      # {"action": "ignore" | "unignore"}
bot_replies: set[int] = set()     # id наших ответов в Избранном, чтобы не принять их за ввод


async def say(client: Client, text: str) -> None:
    msg = await client.send_message("me", text)
    bot_replies.add(msg.id)


async def resolve(client: Client, token: str) -> tuple[int | None, str | None, str]:
    """@username, username, ссылка t.me/username или числовой id -> (id, username, подпись)."""
    token = token.strip().strip(",")
    for prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if token.startswith(prefix):
            token = token[len(prefix):]
    token = token.lstrip("@")
    if token.lstrip("-").isdigit():
        uid = int(token)
        try:
            u = await client.get_users(uid)
            return uid, u.username, f"{u.first_name or ''} (@{u.username})" if u.username else str(uid)
        except Exception:
            return uid, None, str(uid)
    try:
        u = await client.get_users(token)
        return u.id, u.username or token, f"@{u.username or token}"
    except Exception:
        # Не нашли через Telegram — сохраняем хотя бы username
        return None, token, f"@{token} (не найден в Telegram, сохранил по username)"


async def apply_action(client: Client, action: str, tokens: list[str]) -> None:
    lines = []
    for tok in tokens:
        if not tok.strip():
            continue
        uid, uname, label = await resolve(client, tok)
        if action == "ignore":
            added = await storage.add_ignored(uid, uname)
            lines.append(f"✅ {label} — больше не отвечаю" if added else f"ℹ️ {label} уже в списке")
        else:
            removed = await storage.remove_ignored(uid, uname)
            lines.append(f"✅ {label} — убран из списка" if removed else f"ℹ️ {label} не было в списке")
    await say(client, "\n".join(lines) or "Не увидел ни одного username")


# ---------------------------------------------------------------------------
# /test и /stats
# ---------------------------------------------------------------------------

_channel_cache: dict[str, int] = {}


async def get_channel_id(client: Client) -> tuple[int | None, str]:
    """Находит канал по ссылке. Возвращает (id, пояснение)."""
    if "id" in _channel_cache:
        return _channel_cache["id"], ""
    try:
        chat = await client.get_chat(CHANNEL_LINK)
    except Exception as e:
        return None, f"не удалось открыть канал по ссылке ({e.__class__.__name__})"
    if not isinstance(chat, Chat):
        return None, "аккаунт не состоит в канале, вступите в него"
    _channel_cache["id"] = chat.id
    return chat.id, ""


def utc_str(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


async def cmd_test(client: Client) -> None:
    lines = ["🩺 Проверка бота"]
    t0 = time.monotonic()
    try:
        me = await client.get_me()
        ping = (time.monotonic() - t0) * 1000
        lines.append(f"✅ Telegram: подключён как {me.first_name}, ответ за {ping:.0f} мс")
    except Exception as e:
        lines.append(f"❌ Telegram: {e}")
    try:
        lines.append(
            f"✅ База: {await storage.count()} собеседников, "
            f"{await storage.count_ignored()} в «не отвечать»"
        )
    except Exception as e:
        lines.append(f"❌ База: {e}")
    lines.append(f"✅ Диалогов в работе сейчас: {len(tasks)}")
    cid, why = await get_channel_id(client)
    lines.append(f"✅ Канал найден" if cid else f"⚠️ Канал: {why}")
    up = int(time.time() - STARTED_AT)
    lines.append(f"⏱ Работает без перезапуска: {up // 3600} ч {up % 3600 // 60} мин")
    await say(client, "\n".join(lines))


async def count_in_channel(client: Client, users: list[tuple[int, str]]) -> str:
    if not users:
        return "—"
    cid, why = await get_channel_id(client)
    if not cid:
        return f"не проверить: {why}"
    inside = 0
    for uid, _ in users[:300]:
        try:
            m = await client.get_chat_member(cid, uid)
            if m.status not in (enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED):
                inside += 1
        except UserNotParticipant:
            pass
        except ChatAdminRequired:
            return "не проверить: список подписчиков видит только админ канала"
        except Exception:
            pass
        await asyncio.sleep(0.3)  # бережём аккаунт от лимитов Telegram
    note = " (проверены первые 300)" if len(users) > 300 else ""
    return f"{inside} из {len(users)}{note}"


async def cmd_stats(client: Client) -> None:
    now = datetime.now(TZ)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    week = today - timedelta(days=6)

    t = await storage.stats(utc_str(today))
    y = await storage.stats(utc_str(yesterday), utc_str(today))
    w = await storage.stats(utc_str(week))
    a = await storage.stats("0000-01-01")

    await say(client, "⏳ Считаю, проверяю подписки на канал…")
    in_today = await count_in_channel(client, await storage.users_since(utc_str(today)))
    in_week = await count_in_channel(client, await storage.users_since(utc_str(week)))

    def row(label: str, d: dict) -> str:
        return f"{label}: {d['total']} (ответ получили {d['first']}, совет с каналом {d['second']})"

    text = (
        f"📊 Статистика на {now:%d.%m %H:%M}\n\n"
        f"{row('Сегодня', t)}\n"
        f"{row('Вчера', y)}\n"
        f"{row('За 7 дней', w)}\n"
        f"{row('Всего', a)}\n\n"
        f"В канале из написавших сегодня: {in_today}\n"
        f"В канале из написавших за 7 дней: {in_week}\n\n"
        f"В «не отвечать»: {await storage.count_ignored()}"
    )
    await say(client, text)


@app.on_message(filters.me & filters.private & filters.text)
async def on_my_message(client: Client, message: Message) -> None:
    if message.id in bot_replies:
        return
    text = message.text.strip()
    is_saved = message.chat.id == client.me.id

    # /ignore прямо в чате с человеком
    if not is_saved:
        if text.lower() == "/ignore" and message.chat.type == enums.ChatType.PRIVATE:
            peer = message.chat
            await storage.add_ignored(peer.id, peer.username)
            try:
                await message.delete()
            except Exception:
                pass
            label = f"@{peer.username}" if peer.username else (peer.first_name or str(peer.id))
            await say(client, f"✅ {label} — больше не отвечаю")
        return

    parts = text.split()
    cmd = parts[0].lower().split("@")[0] if parts else ""

    if cmd in ("/help", "/start"):
        await say(client, HELP_TEXT)
    elif cmd in ("/ignore", "/unignore"):
        action = cmd[1:]
        if len(parts) > 1:
            awaiting.clear()
            await apply_action(client, action, parts[1:])
        else:
            awaiting["action"] = action
            verb = "не отвечать" if action == "ignore" else "убрать из списка"
            await say(client, f"Пришли username, кому {verb} (можно несколько через пробел). /cancel — отмена")
    elif cmd == "/ignorelist":
        rows = await storage.list_ignored()
        if not rows:
            await say(client, "Список «не отвечать» пуст")
        else:
            body = "\n".join(
                f"{i}. {('@' + n) if n and not n.isdigit() and ' ' not in n else (n or '')} {('id ' + str(uid)) if uid else ''}".strip()
                for i, (uid, n, _) in enumerate(rows, 1)
            )
            # Telegram ограничивает сообщение 4096 символами
            for chunk_start in range(0, len(body), 4000):
                await say(client, f"Не отвечаю ({len(rows)}):\n" + body[chunk_start:chunk_start + 4000])
    elif cmd == "/test":
        await cmd_test(client)
    elif cmd == "/stats":
        await cmd_stats(client)
    elif cmd == "/cancel":
        awaiting.clear()
        await say(client, "Ок, отменил")
    elif "action" in awaiting and not text.startswith("/"):
        action = awaiting.pop("action")
        await apply_action(client, action, text.replace(",", " ").split())


async def main() -> None:
    if not API_ID or not API_HASH:
        raise SystemExit("Заполните API_ID и API_HASH в файле .env")

    await storage.connect()
    await app.start()
    log.info("Бот запущен. В базе %d пользователей.", await storage.count())
    log.info("Команды управления: напишите /help себе в «Избранное»")

    # Досылаем то, что не успели до прошлой остановки
    for user_id, chat_id, name, stage in await storage.unfinished():
        if await storage.is_ignored(user_id, name):
            continue
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
