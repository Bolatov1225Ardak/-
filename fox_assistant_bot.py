# -*- coding: utf-8 -*-
"""
fox_assistant_bot.py — Telegram-бот (aiogram 3 + APScheduler).

Что умеет:
  • /fox, /daily_on, /daily_off, /daily_status — фото лис (в т.ч. ежедневная рассылка);
  • /remind, /reminders, /cancel_reminder      — личные напоминания (хранятся в SQLite);
  • /schedule, /today, /next                   — расписание занятий;
  • /lessons_on, /lessons_off, /lessons_status — АВТОНАПОМИНАНИЯ О ПАРАХ
                                                 (за N минут до начала, по умолчанию 10);
  • любое обычное сообщение в личке            — вопрос ИИ через OpenRouter.

Как устроены напоминания о парах (важно для стабильности):
  1. Каждые 15 секунд планировщик (APScheduler) смотрит: не пора ли напомнить?
  2. Напоминание уходит, когда  (начало пары − N минут) <= сейчас < начало пары.
     Если бот был выключен и проснулся за 3 минуты до пары — он всё равно напомнит
     («Через 3 мин»), а если пара уже началась — тихо пропустит (поздно).
  3. Чтобы не было дублей, каждая отправка «занимает» строку в таблице
     lesson_notifications (чат + дата + пара). Перезапуск бота дублей не создаёт.

Настройки берутся из переменных окружения или файла bot.env рядом со скриптом.
Нужен Python 3.10+.
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import math
import os
import re
import sqlite3
import time
from contextlib import closing, suppress
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Awaitable, Callable
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import BaseMiddleware, Bot, Dispatcher, F
from aiogram.enums import ChatAction, ParseMode
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
    TelegramUnauthorizedError,
)
from aiogram.filters import Command, CommandObject
from aiogram.types import ErrorEvent, Message
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# ════════════════════════════════════════════════════════════════════════════
# 1. НАСТРОЙКИ
# ════════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / "bot.env")  # на хостинге переменные задаются в панели, файл не нужен

HHMM_RE = re.compile(r"(?:[01]\d|2[0-3]):[0-5]\d")


def env_str(name: str, default: str = "") -> str:
    """Читает переменную окружения как строку без пробелов по краям."""
    return os.getenv(name, default).strip()


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    """Читает целое число из окружения и ограничивает его диапазоном."""
    raw = env_str(name)
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def parse_id_list(raw: str) -> set[int]:
    """'123, 456;789' -> {123, 456, 789}. Мусор молча пропускается."""
    result: set[int] = set()
    for chunk in re.split(r"[,\s;]+", raw):
        if re.fullmatch(r"-?[0-9]+", chunk):
            result.add(int(chunk))
    return result


BOT_TOKEN = env_str("BOT_TOKEN")
OPENROUTER_API_KEY = env_str("OPENROUTER_API_KEY")
AI_MODEL = env_str("OPENROUTER_MODEL", "openrouter/free")
TIMEZONE_NAME = env_str("BOT_TIMEZONE", "Asia/Almaty")
DAILY_DEFAULT_TIME = env_str("DAILY_FOX_TIME", "09:00")

# Кто может пользоваться ботом. Пусто = бот открыт для всех (НЕ рекомендуется:
# чужие люди будут тратить твой лимит OpenRouter). Узнать свой ID: бот @userinfobot.
ALLOWED_USER_IDS = parse_id_list(env_str("ALLOWED_USER_IDS"))

# За сколько минут до пары напоминать (по умолчанию для новых подписок).
LESSON_REMIND_MINUTES = env_int("LESSON_REMIND_MINUTES", 10, 1, 120)

# Время «0 пары» (физкультура) в расписании не указано. Если знаешь — впиши HH:MM,
# и бот будет напоминать и о ней. Пусто = без напоминания (в /schedule она видна).
ZERO_PAIR_TIME = env_str("ZERO_PAIR_TIME")

# Где лежит база. На хостингах с постоянным диском можно указать путь на этот диск.
DB_PATH = Path(env_str("DB_PATH")) if env_str("DB_PATH") else BASE_DIR / "bot_data.sqlite3"

# Для облачных хостингов (Render): порт для мини-сервера «я жив» и публичный адрес.
PORT = env_int("PORT", 0, 0, 65535)
PUBLIC_URL = (env_str("RENDER_EXTERNAL_URL") or env_str("PUBLIC_URL")).rstrip("/")

FOX_API_URL = "https://randomfox.ca/floof/"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

MAX_REMINDER_TEXT = 500        # максимум символов в тексте напоминания
MAX_REMINDERS_PER_CHAT = 50    # максимум активных напоминаний на чат
MAX_AI_INPUT = 4000            # максимум символов в вопросе ИИ
AI_COOLDOWN_SECONDS = 3.0      # пауза между вопросами ИИ от одного пользователя
LATE_NOTICE_SECONDS = 300      # после какого опоздания добавлять пометку в напоминание
DAILY_CATCHUP_MINUTES = 180    # сколько после 09:00 ещё можно отправить «ежедневную лису»

if not BOT_TOKEN:
    raise ValueError(
        "Не найден BOT_TOKEN. Укажи токен BotFather в bot.env или в переменных окружения."
    )

try:
    LOCAL_TZ = ZoneInfo(TIMEZONE_NAME)
except Exception as exc:  # noqa: BLE001 — нужна понятная ошибка для новичка
    raise ValueError(
        f"Не удалось загрузить часовой пояс {TIMEZONE_NAME!r}. "
        "Проверь BOT_TIMEZONE и что установлен пакет tzdata (есть в requirements.txt)."
    ) from exc

if not HHMM_RE.fullmatch(DAILY_DEFAULT_TIME):
    DAILY_DEFAULT_TIME = "09:00"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("fox_bot")

try:
    bot = Bot(token=BOT_TOKEN)
except Exception as exc:  # noqa: BLE001
    raise ValueError(
        "BOT_TOKEN выглядит неверно. Он должен иметь вид 123456789:AAAA... (из @BotFather)."
    ) from exc
dp = Dispatcher()

DURATION_PATTERN = re.compile(r"([0-9]{1,6})([smhd])", re.IGNORECASE)
DURATION_FACTORS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


# ════════════════════════════════════════════════════════════════════════════
# 2. РАСПИСАНИЕ ЗАНЯТИЙ
# ════════════════════════════════════════════════════════════════════════════

WEEKDAY_NAMES = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]


def hhmm(value: str) -> dtime:
    """'14:10' -> datetime.time(14, 10)."""
    hours, minutes = value.split(":")
    return dtime(int(hours), int(minutes))


@dataclass(frozen=True)
class Lesson:
    """Одна пара / урок. start=None означает «время не задано» (напоминаний не будет)."""

    label: str                    # «1-я пара», «0 пара», «Урок»
    title: str                    # «Python | АБСП»
    emoji: str
    start: dtime | None = None
    end: dtime | None = None

    @property
    def key(self) -> str:
        """Стабильный ключ для защиты от повторных напоминаний."""
        start = f"{self.start:%H:%M}" if self.start else "--:--"
        return f"{self.label}|{self.title}|{start}"


# Звонки пар (одинаковые каждый день).
PAIR_SLOTS = {1: ("14:10", "14:55"), 2: ("15:05", "15:50"), 3: ("16:00", "16:45")}

# Предметы: (название, эмодзи).
PYTHON = ("Python | АБСП", "🐍")
POSTGRES = ("PostgreSQL | ДМБС", "🐘")
CSHARP = ("C# | ИКТ", "💻")
ECONOMICS = ("Экономика", "📈")


def pair(number: int, subject: tuple[str, str]) -> Lesson:
    start, end = PAIR_SLOTS[number]
    return Lesson(f"{number}-я пара", subject[0], subject[1], hhmm(start), hhmm(end))


_zero_start = hhmm(ZERO_PAIR_TIME) if HHMM_RE.fullmatch(ZERO_PAIR_TIME) else None
PHYS_ED = Lesson("0 пара", "Физкультура", "🏃", _zero_start)
ENGLISH = Lesson("Урок", "Английский язык", "🇬🇧", hhmm("13:30"))

# Ключ: 0 = понедельник … 4 = пятница. В выходные пар нет.
SCHEDULE: dict[int, tuple[Lesson, ...]] = {
    0: (PHYS_ED, pair(1, PYTHON), pair(2, PYTHON), pair(3, POSTGRES)),
    1: (pair(1, PYTHON), pair(2, PYTHON), pair(3, POSTGRES)),
    2: (pair(1, PYTHON), pair(2, CSHARP), pair(3, POSTGRES)),
    3: (ENGLISH, pair(1, POSTGRES), pair(2, CSHARP), pair(3, PYTHON)),
    4: (PHYS_ED, pair(1, ECONOMICS), pair(2, CSHARP), pair(3, POSTGRES)),
}


def lesson_start(day: date, lesson: Lesson) -> datetime | None:
    """Момент начала пары в локальном часовом поясе (или None, если время не задано)."""
    if lesson.start is None:
        return None
    return datetime.combine(day, lesson.start, tzinfo=LOCAL_TZ)


def lessons_due_for_reminder(now: datetime, lead_minutes: int) -> list[tuple[Lesson, int]]:
    """Пары СЕГОДНЯ, о которых пора напомнить: (начало − lead) <= сейчас < начало.

    Возвращает список (пара, сколько минут осталось до начала).
    """
    result: list[tuple[Lesson, int]] = []
    for lesson in SCHEDULE.get(now.weekday(), ()):
        start_dt = lesson_start(now.date(), lesson)
        if start_dt is None:
            continue
        remind_at = start_dt - timedelta(minutes=lead_minutes)
        if remind_at <= now < start_dt:
            minutes_left = max(1, math.ceil((start_dt - now).total_seconds() / 60))
            result.append((lesson, minutes_left))
    return result


def find_next_lesson(now: datetime) -> tuple[datetime, Lesson] | None:
    """Ближайшая будущая пара с известным временем (смотрим на неделю вперёд)."""
    for offset in range(8):
        day = now.date() + timedelta(days=offset)
        candidates = []
        for lesson in SCHEDULE.get(day.weekday(), ()):
            start_dt = lesson_start(day, lesson)
            if start_dt is not None and start_dt > now:
                candidates.append((start_dt, lesson))
        if candidates:
            return min(candidates, key=lambda item: item[0])
    return None


def time_range(lesson: Lesson) -> str:
    """'14:10–14:55', '13:30' или '' если времени нет."""
    if lesson.start is None:
        return ""
    start = f"{lesson.start:%H:%M}"
    return f"{start}–{lesson.end:%H:%M}" if lesson.end else start


def format_lesson_line(lesson: Lesson) -> str:
    line = f"<b>{html.escape(lesson.label)}</b> — {lesson.emoji} {html.escape(lesson.title)}"
    rng = time_range(lesson)
    return f"{line} [{rng}]" if rng else line


def format_day(weekday: int) -> str:
    lessons = SCHEDULE.get(weekday, ())
    title = f"<b>{WEEKDAY_NAMES[weekday]}</b>"
    if not lessons:
        return f"{title}\nПар нет 🎉"
    return title + "\n" + "\n".join(format_lesson_line(lesson) for lesson in lessons)


def format_delta(delta: timedelta) -> str:
    """timedelta -> '1 д 3 ч 20 мин'."""
    total_minutes = max(0, int(delta.total_seconds() // 60))
    days, rest = divmod(total_minutes, 1440)
    hours, minutes = divmod(rest, 60)
    parts = []
    if days:
        parts.append(f"{days} д")
    if hours:
        parts.append(f"{hours} ч")
    if minutes or not parts:
        parts.append(f"{minutes} мин")
    return " ".join(parts)


def format_next_lesson(now: datetime) -> str:
    found = find_next_lesson(now)
    if not found:
        return "Ближайших пар с известным временем нет."
    start_dt, lesson = found
    return (
        f"⏭ Ближайшая: <b>{WEEKDAY_NAMES[start_dt.weekday()]}, {start_dt:%H:%M}</b> "
        f"(через {format_delta(start_dt - now)})\n"
        f"{lesson.emoji} {html.escape(lesson.title)} — {html.escape(lesson.label)}"
    )


def format_lesson_reminder(lesson: Lesson, minutes_left: int) -> str:
    return (
        f"🔔 <b>Через {minutes_left} мин — {html.escape(lesson.label)}</b>\n"
        f"{lesson.emoji} {html.escape(lesson.title)}\n"
        f"🕑 {time_range(lesson)}"
    )


# ════════════════════════════════════════════════════════════════════════════
# 3. БАЗА ДАННЫХ (SQLite; вызовы вынесены в потоки, чтобы не блокировать бота)
# ════════════════════════════════════════════════════════════════════════════

def _connect() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


def _init_db_sync() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with closing(_connect()) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS daily_subscriptions (
                chat_id INTEGER PRIMARY KEY,
                send_time TEXT NOT NULL,
                last_sent_date TEXT
            );
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                due_at REAL NOT NULL,
                text TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_reminders_due ON reminders (due_at);
            CREATE TABLE IF NOT EXISTS lesson_subscriptions (
                chat_id INTEGER PRIMARY KEY,
                lead_minutes INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS lesson_notifications (
                chat_id INTEGER NOT NULL,
                day TEXT NOT NULL,
                lesson_key TEXT NOT NULL,
                PRIMARY KEY (chat_id, day, lesson_key)
            );
            """
        )
        connection.commit()


def _query_sync(sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    with closing(_connect()) as connection:
        return connection.execute(sql, params).fetchall()


def _execute_sync(sql: str, params: tuple = ()) -> tuple[int, int]:
    with closing(_connect()) as connection:
        cursor = connection.execute(sql, params)
        connection.commit()
        return cursor.lastrowid or 0, cursor.rowcount


async def init_db() -> None:
    await asyncio.to_thread(_init_db_sync)


async def db_query(sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    return await asyncio.to_thread(_query_sync, sql, params)


async def db_execute(sql: str, params: tuple = ()) -> tuple[int, int]:
    """Возвращает (lastrowid, rowcount)."""
    return await asyncio.to_thread(_execute_sync, sql, params)


async def restore_env_subscribers() -> None:
    """Подписывает ALLOWED_USER_IDS на напоминания о парах при каждом запуске.

    Зачем: на бесплатных хостингах диск может стираться при перезапуске. Так ты
    не потеряешь напоминания о парах, даже если база пропала.
    """
    for user_id in sorted(ALLOWED_USER_IDS):
        if user_id > 0:  # в личке chat_id == user_id
            await db_execute(
                "INSERT OR IGNORE INTO lesson_subscriptions (chat_id, lead_minutes) VALUES (?, ?)",
                (user_id, LESSON_REMIND_MINUTES),
            )


async def cleanup_old_notifications() -> None:
    """Удаляет записи о старых напоминаниях (старше 7 дней), чтобы база не пухла."""
    border = (datetime.now(LOCAL_TZ).date() - timedelta(days=7)).isoformat()
    await db_execute("DELETE FROM lesson_notifications WHERE day < ?", (border,))


# ════════════════════════════════════════════════════════════════════════════
# 4. ОТПРАВКА СООБЩЕНИЙ (повторы, flood-control, «чат недоступен»)
# ════════════════════════════════════════════════════════════════════════════

SEND_OK = "ok"        # доставлено
SEND_GONE = "gone"    # чат недоступен (бот заблокирован/чата нет) — повторять бессмысленно
SEND_ERROR = "error"  # временная ошибка — можно попробовать позже


async def send_with_retry(make_call: Callable[[], Awaitable[object]], what: str) -> str:
    """Выполняет отправку с тремя попытками. Возвращает SEND_OK / SEND_GONE / SEND_ERROR."""
    for attempt in range(1, 4):
        try:
            await make_call()
            return SEND_OK
        except TelegramRetryAfter as exc:
            # Telegram просит подождать (flood control).
            await asyncio.sleep(min(exc.retry_after + 1, 30))
        except TelegramForbiddenError:
            return SEND_GONE
        except TelegramBadRequest as exc:
            if "chat not found" in str(exc).lower():
                return SEND_GONE
            logger.warning("Ошибка Telegram при отправке (%s), попытка %s: %s", what, attempt, exc)
            await asyncio.sleep(1)
        except Exception as exc:  # noqa: BLE001 — сеть, тайм-ауты и т.п.
            logger.warning("Сбой отправки (%s), попытка %s: %s", what, attempt, exc)
            await asyncio.sleep(2)
    return SEND_ERROR


async def send_text(chat_id: int, text: str, parse_mode: str | None = None) -> str:
    kwargs = {"parse_mode": parse_mode} if parse_mode else {}
    return await send_with_retry(
        lambda: bot.send_message(chat_id=chat_id, text=text, **kwargs),
        what=f"текст в чат {chat_id}",
    )


# ── HTTP-сессия (одна на весь процесс, а не новая на каждый запрос) ─────────

_http_session: aiohttp.ClientSession | None = None


async def get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
    return _http_session


async def close_http_session() -> None:
    if _http_session is not None and not _http_session.closed:
        await _http_session.close()


# ── Фото лис ────────────────────────────────────────────────────────────────

async def get_fox_image_url() -> str:
    session = await get_http_session()
    async with session.get(FOX_API_URL, timeout=aiohttp.ClientTimeout(total=20)) as response:
        response.raise_for_status()
        data = await response.json(content_type=None)
    image_url = data.get("image") if isinstance(data, dict) else None
    if not image_url:
        raise ValueError("Сервис с фотографиями лис не вернул ссылку на изображение")
    return image_url


async def send_fox_photo(chat_id: int, caption: str) -> str:
    """Берёт свежую ссылку на фото и отправляет. Если Telegram не смог скачать
    картинку, при повторе берётся другая лиса."""

    async def _once() -> object:
        image_url = await get_fox_image_url()
        return await bot.send_photo(chat_id=chat_id, photo=image_url, caption=caption)

    return await send_with_retry(_once, what=f"фото лисы в чат {chat_id}")


# ════════════════════════════════════════════════════════════════════════════
# 5. ДОСТУП И ОБРАБОТКА ОШИБОК
# ════════════════════════════════════════════════════════════════════════════

class AccessMiddleware(BaseMiddleware):
    """Пропускает только пользователей из ALLOWED_USER_IDS (если список задан)."""

    async def __call__(self, handler, event, data):  # type: ignore[no-untyped-def]
        if not ALLOWED_USER_IDS:
            return await handler(event, data)
        user = getattr(event, "from_user", None)
        if user is not None and user.id in ALLOWED_USER_IDS:
            return await handler(event, data)
        logger.warning("Доступ закрыт для пользователя %s", getattr(user, "id", None))
        return None


dp.message.outer_middleware(AccessMiddleware())


@dp.errors()
async def global_error_handler(event: ErrorEvent) -> bool:
    """Любая необработанная ошибка в хендлере попадает сюда: пишем в лог, а бот живёт дальше."""
    logger.error("Необработанная ошибка в хендлере", exc_info=event.exception)
    message = getattr(event.update, "message", None)
    if message is not None:
        with suppress(Exception):
            await message.answer("Что-то пошло не так. Я уже записал ошибку — попробуй ещё раз.")
    return True


# ════════════════════════════════════════════════════════════════════════════
# 6. КОМАНДЫ
# ════════════════════════════════════════════════════════════════════════════

def help_text() -> str:
    return (
        "Привет! Я лисий бот с ИИ 🦊\n\n"
        "📚 Расписание:\n"
        "/schedule — расписание на неделю\n"
        "/today — пары на сегодня\n"
        "/next — ближайшая пара\n"
        f"/lessons_on — напоминать о парах (по умолчанию за {LESSON_REMIND_MINUTES} мин)\n"
        "/lessons_on 15 — напоминать за 15 минут\n"
        "/lessons_off — выключить напоминания о парах\n"
        "/lessons_status — текущие настройки\n\n"
        "🦊 Лисы:\n"
        "/fox — случайное фото лисы\n"
        f"/daily_on — фото лисы каждый день в {DAILY_DEFAULT_TIME}\n"
        "/daily_on 08:30 — другое время\n"
        "/daily_off — отключить\n"
        "/daily_status — настройки ежедневных фото\n\n"
        "⏰ Личные напоминания:\n"
        "/remind 10m Текст — через 10 минут\n"
        "/remind 2h Текст — через 2 часа\n"
        "/remind 2026-10-20 14:30 Текст — в дату и время\n"
        "/reminders — список\n"
        "/cancel_reminder ID — отменить\n\n"
        "Просто напиши сообщение — отвечу через ИИ.\n"
        f"Часовой пояс: {TIMEZONE_NAME}"
    )


@dp.message(Command("start"))
async def start_handler(message: Message) -> None:
    await message.answer(help_text())


@dp.message(Command("help"))
async def help_handler(message: Message) -> None:
    await message.answer(help_text())


# ── Расписание ──────────────────────────────────────────────────────────────

@dp.message(Command("schedule"))
async def schedule_handler(message: Message) -> None:
    text = "📚 <b>Расписание занятий</b>\n\n" + "\n\n".join(format_day(day) for day in range(5))
    await message.answer(text, parse_mode=ParseMode.HTML)


@dp.message(Command("today"))
async def today_handler(message: Message) -> None:
    now = datetime.now(LOCAL_TZ)
    text = format_day(now.weekday()) + "\n\n" + format_next_lesson(now)
    await message.answer(text, parse_mode=ParseMode.HTML)


@dp.message(Command("next"))
async def next_handler(message: Message) -> None:
    await message.answer(format_next_lesson(datetime.now(LOCAL_TZ)), parse_mode=ParseMode.HTML)


@dp.message(Command("lessons_on"))
async def lessons_on_handler(message: Message, command: CommandObject) -> None:
    lead = LESSON_REMIND_MINUTES
    if command.args:
        arg = command.args.strip()
        if not re.fullmatch(r"[0-9]{1,3}", arg) or not 1 <= int(arg) <= 120:
            await message.answer("Укажи число минут от 1 до 120. Пример: /lessons_on 15")
            return
        lead = int(arg)

    await db_execute(
        """
        INSERT INTO lesson_subscriptions (chat_id, lead_minutes) VALUES (?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET lead_minutes = excluded.lead_minutes
        """,
        (message.chat.id, lead),
    )
    await message.answer(
        f"✅ Напоминания о парах включены: буду писать за {lead} мин до начала "
        f"(часовой пояс {TIMEZONE_NAME}).\nПроверить расписание: /schedule"
    )


@dp.message(Command("lessons_off"))
async def lessons_off_handler(message: Message) -> None:
    _, count = await db_execute(
        "DELETE FROM lesson_subscriptions WHERE chat_id = ?", (message.chat.id,)
    )
    note = ""
    if message.chat.id in ALLOWED_USER_IDS:
        note = "\n(Подписка вернётся после перезапуска бота: ты указан в ALLOWED_USER_IDS.)"
    if count:
        await message.answer("Напоминания о парах выключены. Включить: /lessons_on" + note)
    else:
        await message.answer("Напоминания о парах и так выключены. Включить: /lessons_on")


@dp.message(Command("lessons_status"))
async def lessons_status_handler(message: Message) -> None:
    rows = await db_query(
        "SELECT lead_minutes FROM lesson_subscriptions WHERE chat_id = ?", (message.chat.id,)
    )
    if not rows:
        await message.answer("Напоминания о парах выключены. Включить: /lessons_on")
        return
    await message.answer(
        f"Напоминания о парах включены: за {rows[0]['lead_minutes']} мин до начала "
        f"({TIMEZONE_NAME}).\n"
        + ("0 пара без времени — для неё напоминаний нет (см. ZERO_PAIR_TIME)."
           if _zero_start is None else "")
    )


# ── Фото лис ────────────────────────────────────────────────────────────────

@dp.message(Command("fox"))
async def fox_handler(message: Message) -> None:
    status = await send_fox_photo(message.chat.id, "Твоя лиса на сегодня 🦊")
    if status != SEND_OK:
        await message.answer("Не получилось получить фото лисы сейчас. Попробуй ещё раз чуть позже.")


@dp.message(Command("daily_on"))
async def daily_on_handler(message: Message, command: CommandObject) -> None:
    send_time = DAILY_DEFAULT_TIME
    if command.args:
        candidate = command.args.strip()
        if not HHMM_RE.fullmatch(candidate):
            await message.answer(
                f"Неправильный формат времени. Пример: /daily_on 08:30\n"
                f"Время указывается по часовому поясу {TIMEZONE_NAME}."
            )
            return
        send_time = candidate

    now = datetime.now(LOCAL_TZ)
    # Если время сегодня уже прошло — первая лиса придёт завтра, а не «сразу же».
    already_passed = send_time <= now.strftime("%H:%M")
    last_sent = now.date().isoformat() if already_passed else None
    await db_execute(
        """
        INSERT INTO daily_subscriptions (chat_id, send_time, last_sent_date) VALUES (?, ?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET
            send_time = excluded.send_time, last_sent_date = excluded.last_sent_date
        """,
        (message.chat.id, send_time, last_sent),
    )
    when = "завтра" if already_passed else "сегодня"
    await message.answer(
        f"Готово! Первая лиса придёт {when} в {send_time} (часовой пояс {TIMEZONE_NAME}), "
        "дальше — каждый день."
    )


@dp.message(Command("daily_off"))
async def daily_off_handler(message: Message) -> None:
    _, count = await db_execute(
        "DELETE FROM daily_subscriptions WHERE chat_id = ?", (message.chat.id,)
    )
    if count:
        await message.answer("Ежедневные фотографии лисы отключены. 🦊")
    else:
        await message.answer("Ежедневные фотографии сейчас не включены. Используй /daily_on.")


@dp.message(Command("daily_status"))
async def daily_status_handler(message: Message) -> None:
    rows = await db_query(
        "SELECT send_time, last_sent_date FROM daily_subscriptions WHERE chat_id = ?",
        (message.chat.id,),
    )
    if not rows:
        await message.answer("Ежедневные фото отключены. Включить: /daily_on")
        return
    row = rows[0]
    last_sent = row["last_sent_date"] or "ещё не отправлялись"
    await message.answer(
        f"Ежедневные фото включены. Время: {row['send_time']} ({TIMEZONE_NAME}).\n"
        f"Последняя отправка: {last_sent}."
    )


# ── Личные напоминания ──────────────────────────────────────────────────────

REMIND_USAGE = (
    "Формат напоминания:\n"
    "/remind 10m Выпить воду\n"
    "/remind 2h Проверить почту\n"
    "/remind 2026-10-20 14:30 Встреча\n\n"
    "Единицы: s — секунды, m — минуты, h — часы, d — дни."
)


def parse_reminder(args: str, now: datetime) -> tuple[datetime, str]:
    """Разбирает аргументы /remind. При ошибке бросает ValueError с понятным текстом."""
    args = args.strip()
    parts = args.split(maxsplit=2)
    if len(parts) < 2:
        raise ValueError(REMIND_USAGE)

    duration = DURATION_PATTERN.fullmatch(parts[0])
    if duration:
        seconds = int(duration.group(1)) * DURATION_FACTORS[duration.group(2).lower()]
        if seconds < 1 or seconds > 365 * 86400:
            raise ValueError("Укажи срок от 1 секунды до 365 дней.")
        due = now + timedelta(seconds=seconds)
        text = args.split(maxsplit=1)[1].strip()
    else:
        if len(parts) < 3:
            raise ValueError("Не хватает текста. Например: /remind 2026-10-20 14:30 Встреча")
        try:
            due = datetime.strptime(f"{parts[0]} {parts[1]}", "%Y-%m-%d %H:%M").replace(
                tzinfo=LOCAL_TZ
            )
        except ValueError:
            raise ValueError(
                "Не смог разобрать дату. Используй формат: "
                "/remind 2026-10-20 14:30 Текст напоминания"
            ) from None
        text = parts[2].strip()

    if not text:
        raise ValueError("Напиши, о чём нужно напомнить.")
    if len(text) > MAX_REMINDER_TEXT:
        raise ValueError(f"Слишком длинный текст: максимум {MAX_REMINDER_TEXT} символов.")
    if due <= now:
        raise ValueError("Это время уже прошло. Укажи будущую дату и время.")
    return due, text


@dp.message(Command("remind"))
async def remind_handler(message: Message, command: CommandObject) -> None:
    now = datetime.now(LOCAL_TZ)
    try:
        due, text = parse_reminder(command.args or "", now)
    except ValueError as exc:
        await message.answer(str(exc))
        return

    rows = await db_query("SELECT COUNT(*) AS n FROM reminders WHERE chat_id = ?", (message.chat.id,))
    if rows[0]["n"] >= MAX_REMINDERS_PER_CHAT:
        await message.answer(f"Слишком много активных напоминаний (максимум {MAX_REMINDERS_PER_CHAT}).")
        return

    reminder_id, _ = await db_execute(
        "INSERT INTO reminders (chat_id, due_at, text, attempts) VALUES (?, ?, ?, 0)",
        (message.chat.id, due.timestamp(), text),
    )
    await message.answer(
        f"Напоминание #{reminder_id} сохранено.\n"
        f"Когда: {due:%d.%m.%Y %H:%M:%S} ({TIMEZONE_NAME})\n"
        f"Что: {text}"
    )


@dp.message(Command("reminders"))
async def reminders_handler(message: Message) -> None:
    rows = await db_query(
        "SELECT id, due_at, text FROM reminders WHERE chat_id = ? ORDER BY due_at LIMIT 20",
        (message.chat.id,),
    )
    if not rows:
        await message.answer("Активных напоминаний нет. Создать: /remind 10m Текст")
        return
    lines = ["Твои ближайшие напоминания:"]
    for row in rows:
        due = datetime.fromtimestamp(row["due_at"], LOCAL_TZ)
        lines.append(f"#{row['id']} — {due:%d.%m.%Y %H:%M} — {row['text']}")
    lines.append("\nОтменить: /cancel_reminder ID")
    await message.answer("\n".join(lines))


@dp.message(Command("cancel_reminder"))
async def cancel_reminder_handler(message: Message, command: CommandObject) -> None:
    arg = (command.args or "").strip()
    if not re.fullmatch(r"[0-9]{1,12}", arg):
        await message.answer("Укажи номер напоминания. Пример: /cancel_reminder 3")
        return
    _, count = await db_execute(
        "DELETE FROM reminders WHERE id = ? AND chat_id = ?", (int(arg), message.chat.id)
    )
    if count:
        await message.answer(f"Напоминание #{arg} отменено.")
    else:
        await message.answer("Не нашёл активное напоминание с таким номером в этом чате.")


# ── ИИ (OpenRouter) ─────────────────────────────────────────────────────────

class AIServiceError(Exception):
    """Ошибка ИИ с готовым текстом для пользователя."""


AI_SEMAPHORE = asyncio.Semaphore(3)  # не больше 3 одновременных запросов к ИИ
_last_ai_request: dict[int, float] = {}


def split_text(text: str, limit: int = 4000) -> list[str]:
    """Режет длинный текст на куски <= limit символов (лимит Telegram — 4096)."""
    chunks: list[str] = []
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut < limit // 2:
            cut = text.rfind(" ", 0, limit)
        if cut < limit // 2:
            cut = limit
        chunks.append(text[:cut].rstrip())
        text = text[cut:].lstrip()
    if text:
        chunks.append(text)
    return chunks


async def ask_openrouter(prompt: str) -> str:
    payload = {
        "model": AI_MODEL,
        "messages": [
            {
                "role": "system",
                "content": "Ты дружелюбный помощник. Отвечай на русском языке, если пользователь не попросил иначе.",
            },
            {"role": "user", "content": prompt},
        ],
    }
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }
    try:
        session = await get_http_session()
        async with session.post(
            OPENROUTER_URL, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=60)
        ) as response:
            status = response.status
            raw = await response.text()
    except asyncio.TimeoutError:
        logger.warning("Истёк тайм-аут запроса к OpenRouter")
        raise AIServiceError("ИИ отвечает слишком долго. Попробуй ещё раз немного позже.") from None
    except aiohttp.ClientError:
        logger.exception("Сетевая ошибка при обращении к OpenRouter")
        raise AIServiceError("Не получилось связаться с ИИ. Попробуй снова чуть позже.") from None

    try:
        data = json.loads(raw)
    except ValueError:
        data = {}

    if status != 200:
        logger.warning("OpenRouter вернул HTTP %s: %s", status, raw[:300])
        if status == 429:
            raise AIServiceError(
                "Сейчас достигнут лимит запросов или модель перегружена. Попробуй позже."
            )
        if status in (401, 403):
            raise AIServiceError(
                "API не принял ключ или доступ к модели не разрешён. "
                "Проверь OPENROUTER_API_KEY и OPENROUTER_MODEL."
            )
        raise AIServiceError(f"ИИ временно вернул ошибку HTTP {status}. Попробуй позже.")

    if not isinstance(data, dict):
        raise AIServiceError("ИИ вернул непонятный ответ. Попробуй ещё раз.")
    if data.get("error"):
        logger.warning("OpenRouter вернул ошибку в теле ответа: %s", str(data["error"])[:300])
        raise AIServiceError("ИИ временно недоступен. Попробуй позже.")

    answer = ""
    choices = data.get("choices")
    if isinstance(choices, list) and choices and isinstance(choices[0], dict):
        msg = choices[0].get("message")
        content = msg.get("content") if isinstance(msg, dict) else None
        if isinstance(content, str):
            answer = content.strip()
    if not answer:
        raise AIServiceError("ИИ вернул пустой ответ. Попробуй задать вопрос иначе.")
    return answer


@dp.message(F.text.startswith("/"))
async def unknown_command_handler(message: Message) -> None:
    """Сюда попадают команды, которых нет в боте (известные обработаны выше)."""
    if message.chat.type == "private":
        await message.answer("Не знаю такую команду. Список команд: /help")


@dp.message(F.text, F.chat.type == "private")
async def ai_handler(message: Message) -> None:
    """Обычные сообщения в личке уходят ИИ. В группах бот молчит (не тратим лимит)."""
    text = (message.text or "").strip()
    if not text:
        return
    if not OPENROUTER_API_KEY:
        await message.answer("ИИ не настроен: добавь OPENROUTER_API_KEY и перезапусти бота.")
        return

    # Защита от спама: не чаще одного вопроса в несколько секунд.
    user_id = message.from_user.id if message.from_user else message.chat.id
    now_mono = time.monotonic()
    if now_mono - _last_ai_request.get(user_id, 0.0) < AI_COOLDOWN_SECONDS:
        await message.answer("Не так быстро 🦊 Подожди пару секунд и спроси снова.")
        return
    if len(_last_ai_request) > 1000:
        _last_ai_request.clear()
    _last_ai_request[user_id] = now_mono

    with suppress(Exception):
        await bot.send_chat_action(chat_id=message.chat.id, action=ChatAction.TYPING)
    try:
        async with AI_SEMAPHORE:
            answer = await ask_openrouter(text[:MAX_AI_INPUT])
    except AIServiceError as exc:
        await message.answer(str(exc))
        return

    for chunk in split_text(answer)[:3]:
        await message.answer(chunk)


# ════════════════════════════════════════════════════════════════════════════
# 7. ФОНОВЫЕ ЗАДАЧИ (APScheduler)
# ════════════════════════════════════════════════════════════════════════════

async def send_lesson_reminders(now: datetime | None = None) -> None:
    """Главная новая функция: напоминания о парах всем подписчикам."""
    now = now or datetime.now(LOCAL_TZ)
    if now.weekday() not in SCHEDULE:
        return
    day = now.date().isoformat()
    subscribers = await db_query("SELECT chat_id, lead_minutes FROM lesson_subscriptions")

    for sub in subscribers:
        chat_id = sub["chat_id"]
        for lesson, minutes_left in lessons_due_for_reminder(now, sub["lead_minutes"]):
            # «Занимаем» отправку: если строка уже есть — напоминание уже уходило.
            _, claimed = await db_execute(
                "INSERT OR IGNORE INTO lesson_notifications (chat_id, day, lesson_key) VALUES (?, ?, ?)",
                (chat_id, day, lesson.key),
            )
            if claimed != 1:
                continue

            status = await send_text(
                chat_id, format_lesson_reminder(lesson, minutes_left), parse_mode=ParseMode.HTML
            )
            if status == SEND_OK:
                logger.info("Напоминание о паре «%s» отправлено в чат %s", lesson.title, chat_id)
            elif status == SEND_GONE:
                logger.warning("Чат %s недоступен — отключаю напоминания о парах", chat_id)
                await db_execute("DELETE FROM lesson_subscriptions WHERE chat_id = ?", (chat_id,))
                break
            else:
                # Временная ошибка: освобождаем «бронь», в следующий тик попробуем снова
                # (пока пара не началась).
                await db_execute(
                    "DELETE FROM lesson_notifications WHERE chat_id = ? AND day = ? AND lesson_key = ?",
                    (chat_id, day, lesson.key),
                )
            await asyncio.sleep(0.05)


async def send_due_reminders(now_ts: float | None = None) -> None:
    """Личные напоминания из /remind."""
    now_ts = time.time() if now_ts is None else now_ts
    rows = await db_query(
        "SELECT id, chat_id, due_at, text, attempts FROM reminders "
        "WHERE due_at <= ? ORDER BY due_at LIMIT 50",
        (now_ts,),
    )
    for row in rows:
        reminder_id = row["id"]
        text = f"⏰ Напоминание\n\n{row['text']}"
        if now_ts - row["due_at"] > LATE_NOTICE_SECONDS:
            text += "\n\n⚠️ Бот был недоступен, поэтому напоминание пришло с опозданием."

        status = await send_text(row["chat_id"], text)
        if status == SEND_OK:
            await db_execute("DELETE FROM reminders WHERE id = ?", (reminder_id,))
        elif status == SEND_GONE:
            logger.warning("Чат %s недоступен; удаляю напоминание #%s", row["chat_id"], reminder_id)
            await db_execute("DELETE FROM reminders WHERE id = ?", (reminder_id,))
        else:
            attempts = int(row["attempts"]) + 1
            if attempts >= 5:
                logger.error("Напоминание #%s удалено после 5 неудачных попыток", reminder_id)
                await db_execute("DELETE FROM reminders WHERE id = ?", (reminder_id,))
            else:
                await db_execute(
                    "UPDATE reminders SET attempts = ?, due_at = ? WHERE id = ?",
                    (attempts, now_ts + 300, reminder_id),
                )
        await asyncio.sleep(0.05)


async def send_due_daily_foxes(now: datetime | None = None) -> None:
    """Ежедневная лиса. Отправляется в окне [время; время + DAILY_CATCHUP_MINUTES]."""
    now = now or datetime.now(LOCAL_TZ)
    today = now.date().isoformat()
    now_minutes = now.hour * 60 + now.minute
    rows = await db_query(
        "SELECT chat_id, send_time FROM daily_subscriptions "
        "WHERE last_sent_date IS NULL OR last_sent_date <> ?",
        (today,),
    )
    for row in rows:
        try:
            hours, minutes = row["send_time"].split(":")
            late = now_minutes - (int(hours) * 60 + int(minutes))
        except ValueError:
            logger.warning("Некорректное время рассылки у чата %s", row["chat_id"])
            continue
        if late < 0 or late > DAILY_CATCHUP_MINUTES:
            continue  # ещё рано, либо окно упущено (бот был выключен) — ждём завтра

        status = await send_fox_photo(row["chat_id"], "Твоя ежедневная лиса 🦊 Хорошего дня!")
        if status == SEND_OK:
            await db_execute(
                "UPDATE daily_subscriptions SET last_sent_date = ? WHERE chat_id = ?",
                (today, row["chat_id"]),
            )
        elif status == SEND_GONE:
            logger.warning("Чат %s недоступен; отключаю ежедневные фото", row["chat_id"])
            await db_execute("DELETE FROM daily_subscriptions WHERE chat_id = ?", (row["chat_id"],))
        await asyncio.sleep(0.05)


async def keep_alive_ping() -> None:
    """Пингует собственный публичный адрес, чтобы бесплатный хостинг не «усыплял» бота."""
    if not PUBLIC_URL:
        return
    try:
        session = await get_http_session()
        async with session.get(
            f"{PUBLIC_URL}/health", timeout=aiohttp.ClientTimeout(total=15)
        ) as response:
            await response.read()
    except Exception as exc:  # noqa: BLE001
        logger.debug("Самопинг не удался: %s", exc)


def guarded(job: Callable[[], Awaitable[None]], name: str) -> Callable[[], Awaitable[None]]:
    """Оборачивает задачу: ошибка в одной задаче не мешает остальным и планировщику."""

    async def wrapper() -> None:
        try:
            await job()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("Ошибка в фоновой задаче «%s»", name)

    return wrapper


def build_scheduler() -> AsyncIOScheduler:
    # Часовой пояс передаём строкой — так он принимается всеми версиями APScheduler 3.x.
    scheduler = AsyncIOScheduler(timezone=TIMEZONE_NAME)
    common = dict(max_instances=1, coalesce=True, misfire_grace_time=60, replace_existing=True)

    scheduler.add_job(
        guarded(send_lesson_reminders, "напоминания о парах"),
        "interval", seconds=15, id="lesson_reminders", **common,
    )
    scheduler.add_job(
        guarded(send_due_reminders, "личные напоминания"),
        "interval", seconds=15, id="user_reminders", **common,
    )
    scheduler.add_job(
        guarded(send_due_daily_foxes, "ежедневная лиса"),
        "interval", seconds=60, id="daily_fox", **common,
    )
    scheduler.add_job(
        guarded(cleanup_old_notifications, "очистка базы"),
        "interval", hours=6, id="cleanup", **common,
    )
    if PUBLIC_URL and PORT:
        scheduler.add_job(
            guarded(keep_alive_ping, "самопинг"),
            "interval", minutes=9, id="keep_alive", **common,
        )
    return scheduler


# ════════════════════════════════════════════════════════════════════════════
# 8. МИНИ-СЕРВЕР «Я ЖИВ» (нужен бесплатным хостингам типа Render)
# ════════════════════════════════════════════════════════════════════════════

async def start_health_server() -> web.AppRunner:
    async def health(_request: web.Request) -> web.Response:
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app, access_log=None)  # access_log=None: не засорять лог пингами
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logger.info("Мини-сервер «я жив» запущен на порту %s", PORT)
    return runner


# ════════════════════════════════════════════════════════════════════════════
# 9. ЗАПУСК
# ════════════════════════════════════════════════════════════════════════════

async def connect_to_telegram():  # type: ignore[no-untyped-def]
    """Проверяет токен. При временных сбоях сети повторяет попытки, а не падает."""
    delay = 3
    while True:
        try:
            return await bot.get_me()
        except TelegramUnauthorizedError:
            raise ValueError("Telegram не принял BOT_TOKEN. Проверь токен в @BotFather.") from None
        except Exception as exc:  # noqa: BLE001
            logger.warning("Нет связи с Telegram (%s). Повтор через %s с...", exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)


async def main() -> None:
    await init_db()
    await restore_env_subscribers()

    health_runner = await start_health_server() if PORT else None
    await get_http_session()

    me = await connect_to_telegram()
    # Если когда-то был включён webhook — polling не заработает. Pending-обновления
    # НЕ сбрасываем: команды, присланные пока бот спал, будут обработаны.
    with suppress(Exception):
        await bot.delete_webhook(drop_pending_updates=False)

    logger.info("Бот @%s успешно запущен.", me.username)
    logger.info("Модель ИИ: %s | Часовой пояс: %s", AI_MODEL, TIMEZONE_NAME)
    logger.info("База данных: %s", DB_PATH)
    if ALLOWED_USER_IDS:
        logger.info("Доступ разрешён пользователям: %s", sorted(ALLOWED_USER_IDS))
    else:
        logger.warning(
            "ALLOWED_USER_IDS не задан: бот открыт ДЛЯ ВСЕХ. "
            "Укажи свой Telegram ID, чтобы закрыть доступ."
        )

    scheduler = build_scheduler()
    scheduler.start()
    logger.info("Планировщик запущен: напоминания о парах, личные напоминания, ежедневные лисы.")

    try:
        await dp.start_polling(bot)
    finally:
        if scheduler.running:
            scheduler.shutdown(wait=False)
        if health_runner is not None:
            await health_runner.cleanup()
        await close_http_session()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен.")
