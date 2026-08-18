import asyncio
import json
import os
import logging
import re
import html
import unicodedata
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from pymongo import MongoClient
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    PicklePersistence,
    filters,
    ContextTypes,
)
from pyrogram import Client
from pyrogram.errors import (
    PeerIdInvalid, ChannelInvalid, UsernameInvalid,
    UsernameNotOccupied, FloodWait,
)

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ==============================================================
#  الاعدادات - كلها من متغيرات البيئة (Secrets)
# ==============================================================
BOT_TOKEN    = os.environ["BOT_TOKEN"]
OWNER_ID     = int(os.environ["OWNER_ID"])
API_ID       = int(os.environ["API_ID"])
API_HASH     = os.environ["API_HASH"]
SESSION_STR  = os.environ["SESSION_STR"]
GROUP_ID     = int(os.environ["GROUP_ID"])  # المجموعة الوحيدة التي يعمل فيها البوت
RELAY_CHAT_ID = int(os.environ.get("RELAY_CHAT_ID") or "0")  # معرف مجموعة الريلاي
MONGODB_URI  = os.environ["MONGODB_URI"].strip()
MONGODB_DB   = os.environ.get("MONGODB_DB", "book_bot").strip()

# القنوات مفصولة بفاصلة: @ch1,https://t.me/ch2,ch3
_ENV_CHANNELS = os.environ.get("CHANNELS", "")

GROUP_LINK = "https://t.me/ZyDeenX"
START_MESSAGE = (
    "🌟 مرحبًا بك في بوت المكتبة\n\n"
    "🔎 البحث متاح داخل المجموعة المحددة فقط.\n"
    "اكتب هناك:\n"
    "بحث مكتبة <اسم المكتبة>\n\n"
    "اضغط الزر بالأسفل للانتقال إلى المجموعة."
)

DATA_FILE        = "data.json"
RESULTS_PER_PAGE = 10
RELAY_TIMEOUT    = 60
SEARCH_LIMIT     = 500
MAX_SEARCH_SESSIONS = 20

# كلمات وصفية لا تدخل في اسم الكتاب نفسه عند بداية البحث.
_SEARCH_PREFIX_WORDS = {
    "مكتبة", "المكتبة",
}

GENERIC_ERROR_MESSAGE = "⌯ حدثت مشكلة"

# هذا البوت يعتمد على الشبكة أكثر من اعتماده على حسابات CPU ثقيلة.
# لذلك نزيد عمّال I/O تلقائيًا بحسب الأنوية المتاحة بدل تشغيل نسخ متعددة
# من البوت، لأن تشغيل أكثر من نسخة بنفس BOT_TOKEN يسبب تعارضًا في polling.
_CPU_COUNT = os.cpu_count() or 1
PYROGRAM_WORKERS = max(4, min(32, _CPU_COUNT * 4))
UPDATE_WORKERS = max(4, min(64, _CPU_COUNT * 4))
_SEARCH_LOCK = asyncio.Lock()

_data_cache: Optional[dict] = None
_mongo_client = MongoClient(
    MONGODB_URI,
    serverSelectionTimeoutMS=10000,
    connectTimeoutMS=10000,
)
_mongo_db = _mongo_client[MONGODB_DB]
_settings_collection = _mongo_db["settings"]
_searches_collection = _mongo_db["search_sessions"]


def init_mongodb() -> None:
    """يتحقق من اتصال MongoDB وينشئ فهرس انتهاء الجلسات."""
    _mongo_client.admin.command("ping")
    _searches_collection.create_index(
        "created_at",
        expireAfterSeconds=30 * 24 * 60 * 60,
    )
    logger.info("MongoDB connected")


# -- مساعدة: تحليل رابط/اسم قناة ---------------------------------
def normalize_channel(text: str) -> str:
    text = text.strip()
    if text.startswith("https://t.me/"):
        text = "@" + text.replace("https://t.me/", "").split("/")[0].rstrip("/")
    elif text.startswith("t.me/"):
        text = "@" + text.replace("t.me/", "").split("/")[0].rstrip("/")
    elif not text.startswith("@"):
        text = "@" + text
    return text


def parse_env_channels() -> list:
    if not _ENV_CHANNELS.strip():
        return []
    result = []
    for part in _ENV_CHANNELS.split(","):
        ch = normalize_channel(part)
        if ch and ch not in result:
            result.append(ch)
    return result


def sync_channels_from_env() -> dict:
    """يجعل Secret CHANNELS المصدر الوحيد للقنوات المستخدمة."""
    data = load_data()
    configured_channels = parse_env_channels()
    old_ids = data.get("channel_ids", {})

    # حذف القنوات القديمة نهائيًا من قائمة التشغيل، مع الاحتفاظ فقط
    # بالمعرّفات التي تخص القنوات الموجودة حاليًا في Secret CHANNELS.
    data["channels"] = configured_channels
    data["channel_ids"] = {
        channel: old_ids[channel]
        for channel in configured_channels
        if channel in old_ids
    }
    save_data(data)

    logger.info(
        "Using channels from CHANNELS secret: "
        + (", ".join(configured_channels) if configured_channels else "(none)")
    )
    return data


# -- البيانات -------------------------------------------------------
def load_data() -> dict:
    global _data_cache
    if _data_cache is not None:
        return _data_cache
    document = _settings_collection.find_one({"_id": "app"})
    if document:
        _data_cache = {
            "channels": document.get("channels", []),
            "channel_ids": document.get("channel_ids", {}),
        }
    else:
        # نقل البيانات القديمة مرة واحدة إن وُجد ملف data.json.
        _data_cache = {"channels": [], "channel_ids": {}}
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, "r", encoding="utf-8") as file:
                    old_data = json.load(file)
                _data_cache = {
                    "channels": old_data.get("channels", []),
                    "channel_ids": old_data.get("channel_ids", {}),
                }
                logger.info("Migrated data.json to MongoDB")
            except (OSError, json.JSONDecodeError) as error:
                logger.warning(f"Could not migrate data.json: {error}")
        _settings_collection.insert_one({
            "_id": "app",
            **_data_cache,
        })
    return _data_cache


def save_data(data: dict) -> None:
    global _data_cache
    _data_cache = data
    _settings_collection.replace_one(
        {"_id": "app"},
        {
            "_id": "app",
            "channels": data.get("channels", []),
            "channel_ids": data.get("channel_ids", {}),
        },
        upsert=True,
    )


def save_search_session(
    search_id: str,
    query: str,
    results: list,
) -> None:
    """يحفظ نتائج البحث بشكل مشترك حتى تعمل الأزرار لجميع أعضاء المحادثة."""
    _searches_collection.replace_one(
        {"_id": search_id},
        {
            "_id": search_id,
            "search_id": search_id,
            "query": query,
            "results": results,
            "created_at": datetime.now(timezone.utc),
        },
        upsert=True,
    )
    old_sessions = _searches_collection.find(
        {},
        {"_id": 1},
    ).sort("created_at", -1).skip(MAX_SEARCH_SESSIONS)
    old_ids = [item["_id"] for item in old_sessions]
    if old_ids:
        _searches_collection.delete_many({"_id": {"$in": old_ids}})


def load_search_session(search_id: str) -> Optional[dict]:
    return _searches_collection.find_one({"search_id": search_id})


def is_pdf(msg) -> bool:
    if not msg.document:
        return False
    doc = msg.document
    if doc.mime_type and doc.mime_type == "application/pdf":
        return True
    if doc.file_name and doc.file_name.lower().endswith(".pdf"):
        return True
    return False


# -- Pyrogram -------------------------------------------------------
pyro: Optional[Client] = None




async def resolve_channel(username: str) -> Optional[int]:
    if not pyro or not pyro.is_connected:
        return None
    data = load_data()
    ids  = data.setdefault("channel_ids", {})
    if username in ids:
        return ids[username]
    try:
        chat = await asyncio.wait_for(pyro.get_chat(username), timeout=20)
        ids[username] = chat.id
        save_data(data)
        logger.info(f"Resolved {username} -> {chat.id}")
        return chat.id
    except asyncio.TimeoutError:
        logger.warning(f"Timeout resolving {username}")
    except (PeerIdInvalid, ChannelInvalid, UsernameInvalid):
        logger.warning(f"Cannot resolve {username}")
    except FloodWait as e:
        logger.warning(f"FloodWait {e.value}s resolving {username}")
    except Exception as e:
        logger.warning(f"Error resolving {username}: {e}")
    return None


async def start_pyro(app: Application) -> None:
    global pyro
    init_mongodb()
    pyro = Client(
        "book_session",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=SESSION_STR,
        workers=PYROGRAM_WORKERS,
        # لا ينتظر Pyrogram تلقائياً عند FloodWait؛ يعيد الطلب نتيجته
        # مباشرة بدلاً من تعليق البوت لمدة 30 ثانية.
        sleep_threshold=0,
    )
    await pyro.start()
    logger.info("Pyrogram connected - loading dialogs cache...")

    try:
        count = 0
        async def _load():
            nonlocal count
            async for _ in pyro.get_dialogs():
                count += 1
        await asyncio.wait_for(_load(), timeout=30)
        logger.info(f"Loaded {count} dialogs into cache")
    except asyncio.TimeoutError:
        logger.warning("Dialogs load timed out")
    except Exception as e:
        logger.warning(f"Could not load dialogs: {e}")

    # تحميل مجموعة الريلاي من السكريت في الكاش
    try:
        chat = await asyncio.wait_for(pyro.get_chat(RELAY_CHAT_ID), timeout=15)
        logger.info(f"Relay group loaded: {chat.title} ({RELAY_CHAT_ID})")
    except Exception as e:
        logger.warning(f"Could not load relay group: {e}")

    # Secret CHANNELS هو المصدر الوحيد؛ لا ندمج معه MongoDB أو data.json.
    data = sync_channels_from_env()

    # حل معرفات القنوات الموجودة في Secret فقط (بدون انضمام).
    for ch in data.get("channels", []):
        await resolve_channel(ch)
        await asyncio.sleep(0.3)


async def stop_pyro(app: Application) -> None:
    global pyro
    if pyro and pyro.is_connected:
        await pyro.stop()


# -- البحث ---------------------------------------------------------
async def _search_single_channel(ch: str, query: str, ids: dict) -> list:
    peer = ids.get(ch) or ch
    results = []
    try:
        async for msg in pyro.search_messages(peer, query=query, limit=SEARCH_LIMIT):
            if not is_pdf(msg):
                continue
            name = (msg.document.file_name or "").strip()
            if not name and msg.caption:
                name = msg.caption.split("\n")[0].strip()
            if not name:
                name = "ملف PDF"
            results.append({
                "name":   name[:80],
                "chat":   ch,
                "msg_id": msg.id,
            })
    except (PeerIdInvalid, ChannelInvalid, UsernameInvalid, UsernameNotOccupied):
        logger.warning(f"Channel not found or inaccessible, removing: {ch}")
        data = load_data()
        data.get("channel_ids", {}).pop(ch, None)
        save_data(data)
    except FloodWait as e:
        logger.warning(f"FloodWait {e.value}s in {ch} - skipping")
    except Exception as e:
        logger.error(f"Search error ({ch}): {e}")
    return results


async def search_libraries(query: str, channels: list) -> list:
    # Pyrogram uses one session for messages.Search. Running several searches
    # at the same time makes Telegram apply a per-session wait (often 31s).
    # Queue separate user requests, while keeping the channels of one request
    # parallel so a single search remains fast.
    async with _SEARCH_LOCK:
        if not pyro or not pyro.is_connected:
            return []
        data = load_data()
        ids = data.get("channel_ids", {})
        channel_results = await asyncio.gather(
            *[_search_single_channel(channel, query, ids) for channel in channels],
            return_exceptions=False,
        )
        return [result for channel_result in channel_results for result in channel_result]


# -- الريلاي --------------------------------------------------------
async def _do_relay(
    chat: str,
    msg_id: int,
    destination_chat_id: int,
    bot,
    reply_to_message_id: Optional[int] = None,
) -> None:
    relay_msg = await pyro.copy_message(
        chat_id=RELAY_CHAT_ID,
        from_chat_id=chat,
        message_id=msg_id,
        caption="",
    )
    await bot.copy_message(
        chat_id=destination_chat_id,
        from_chat_id=RELAY_CHAT_ID,
        message_id=relay_msg.id,
        caption="",
        reply_to_message_id=reply_to_message_id,
    )
    # تبقى رسالة الكتاب في مجموعة الريلاي ولا يتم حذفها.


async def deliver_via_relay(
    destination_chat_id: int,
    chat: str,
    msg_id: int,
    bot,
    reply_to_message_id: Optional[int] = None,
) -> tuple[bool, str]:
    if not RELAY_CHAT_ID:
        return False, "مجموعة الريلاي غير مضبوطة في السكريت."
    if not pyro or not pyro.is_connected:
        return False, "عميل Pyrogram غير متصل."
    try:
        await asyncio.wait_for(
            _do_relay(
                chat,
                msg_id,
                destination_chat_id,
                bot,
                reply_to_message_id,
            ),
            timeout=RELAY_TIMEOUT,
        )
        return True, ""
    except asyncio.TimeoutError:
        return False, f"انتهت مهلة الإرسال ({RELAY_TIMEOUT}s). حاول مجدداً."
    except Exception as e:
        logger.error(f"Relay error [{chat}/{msg_id}]: {e}")
        return False, str(e)


# -- لوحات المفاتيح ------------------------------------------------
from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def results_keyboard(
    results: list,
    page: int,
    search_id: str,
    owner_user_id: int,
) -> InlineKeyboardMarkup:
    start = page * RESULTS_PER_PAGE
    end   = start + RESULTS_PER_PAGE
    buttons = []
    for abs_idx, r in enumerate(results[start:end], start=start):
        buttons.append([
            InlineKeyboardButton(
                r["name"][:64],
                callback_data=f"sb:{search_id}:{owner_user_id}:{abs_idx}",
            )
        ])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(
            "السابق",
            callback_data=f"pg:{search_id}:{owner_user_id}:{page - 1}",
        ))
    if end < len(results):
        nav.append(InlineKeyboardButton(
            "التالي",
            callback_data=f"pg:{search_id}:{owner_user_id}:{page + 1}",
        ))
    if nav:
        buttons.append(nav)
    return InlineKeyboardMarkup(buttons)


# -- قيود التشغيل ---------------------------------------------------
def is_allowed_group(chat) -> bool:
    return bool(
        chat
        and chat.type in {"group", "supergroup"}
        and chat.id == GROUP_ID
    )


def group_link_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("الدخول إلى المجموعة", url=GROUP_LINK),
    ]])


def user_mention_html(user) -> str:
    """يعرض اسم الحساب كرابط يفتح ملفه الشخصي في تيليجرام."""
    display_name = " ".join(
        part for part in (user.first_name, user.last_name) if part
    ).strip()
    if not display_name and user.username:
        display_name = f"@{user.username}"
    if not display_name:
        display_name = "الحساب"
    return f'<a href="tg://user?id={user.id}">{html.escape(display_name)}</a>'


def results_message(query: str, result_count: int, user) -> str:
    return (
        f"نتائج البحث عن: {html.escape(query)}\n"
        f"عدد النتائج: {result_count}\n"
        f"النتيجة إلى: {user_mention_html(user)}\n\n"
        "اضغط على الملف لاستلامه:"
    )


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """يوجّه المستخدم للمجموعة دون تشغيل البحث في الخاص."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type in {"group", "supergroup"} and not is_allowed_group(chat):
        return
    if chat.type not in {"private", "group", "supergroup"}:
        return
    await update.message.reply_text(
        START_MESSAGE,
        reply_markup=group_link_keyboard(),
        disable_web_page_preview=True,
    )


def extract_group_query(text: str) -> Optional[str]:
    """يستخرج عبارة البحث من صيغة المجموعة: بحث مكتبة <العبارة>."""
    match = re.match(
        r"^\s*بحث\s+مكتبة\s*(?::|：|-)?\s*(.*?)\s*$",
        text,
    )
    query = match.group(1).strip() if match else ""
    return query or None


def _canonical_search_word(word: str) -> str:
    """يوحّد بعض أشكال الحروف العربية لمقارنة الكلمات الوصفية فقط."""
    return word.casefold().translate(str.maketrans({
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ٱ": "ا",
        "ى": "ي",
        "ة": "ه",
        "ؤ": "و",
        "ئ": "ي",
    }))


_CANONICAL_SEARCH_PREFIX_WORDS = {
    _canonical_search_word(word) for word in _SEARCH_PREFIX_WORDS
}


def normalize_search_query(text: str) -> str:
    """
    ينظف عبارة البحث داخل البوت من الرموز والتشكيل والكلمات الوصفية
    الموجودة في بدايتها، مع الإبقاء على اسم المكتبة كما كتبه المستخدم.
    """
    cleaned = []
    for char in text:
        category = unicodedata.category(char)
        if char == "ـ" or category.startswith("M"):
            continue
        if category.startswith(("P", "S", "C")):
            cleaned.append(" ")
        else:
            cleaned.append(char)

    words = "".join(cleaned).split()
    while words and _canonical_search_word(words[0]) in _CANONICAL_SEARCH_PREFIX_WORDS:
        words.pop(0)
    return " ".join(words)


async def handle_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not is_allowed_group(update.effective_chat):
        return
    text = update.message.text.strip()
    query = extract_group_query(text)
    if not query:
        return
    query = normalize_search_query(query)
    if not query:
        return
    data = load_data()
    if not data["channels"]:
        await update.message.reply_text(GENERIC_ERROR_MESSAGE)
        return

    msg     = await update.message.reply_text(f"جاري البحث عن: {query}...")
    results = await search_libraries(query, data["channels"])
    words = query.split()
    if not results and len(words) > 2:
        results = await search_libraries(" ".join(words[:2]), data["channels"])

    # يحتفظ كل بحث بمعرّف مستقل حتى تبقى أزرار كل رسالة مرتبطة بنتائجها.
    search_id = uuid4().hex[:12]
    save_search_session(search_id, query, results)

    if not results:
        await msg.edit_text(f"لم يتم العثور على نتائج لـ: {query}")
        return

    text = results_message(query, len(results), update.effective_user)
    await msg.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=results_keyboard(results, 0, search_id, update.effective_user.id),
    )


async def cb_page(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q    = update.callback_query
    if not q or not is_allowed_group(q.message.chat):
        return
    parts = q.data.split(":")
    if len(parts) >= 4:
        search_id = parts[1]
        owner_user_id = int(parts[2])
        page = int(parts[3])
        if q.from_user.id != owner_user_id:
            await q.answer(
                "هذا البحث ليس لك، ابحث بحثا جديدا",
                show_alert=True,
            )
            return
        await q.answer()
        session = load_search_session(search_id)
        results = session.get("results") if session else None
        query = session.get("query", "") if session else ""
    else:
        await q.answer(
            "هذا البحث ليس لك، ابحث بحثا جديدا",
            show_alert=True,
        )
        results = None
        query = ""
        search_id = ""
        owner_user_id = 0
    if not results:
        await q.message.edit_text("انتهت الجلسة. ابحث مجدداً.")
        return
    text = results_message(query, len(results), q.from_user)
    await q.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=results_keyboard(
            results,
            page,
            search_id,
            owner_user_id,
        ),
    )


async def cb_send_book(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not is_allowed_group(q.message.chat):
        return
    parts = q.data.split(":")
    if len(parts) >= 4:
        search_id = parts[1]
        owner_user_id = int(parts[2])
        idx = int(parts[3])
        if q.from_user.id != owner_user_id:
            await q.answer(
                "هذا البحث ليس لك، ابحث بحثا جديدا",
                show_alert=True,
            )
            return
        await q.answer("جاري الإرسال...")
        session = load_search_session(search_id)
        results = session.get("results", []) if session else []
    else:
        await q.answer(
            "هذا البحث ليس لك، ابحث بحثا جديدا",
            show_alert=True,
        )
        idx = -1
        results = []
    if not results or idx < 0 or idx >= len(results):
        await q.message.reply_text("انتهت الجلسة. ابحث مجدداً.")
        return
    r = results[idx]
    destination_chat_id = q.message.chat.id
    reply_to_message_id = (
        q.message.message_id
        if q.message.chat.type in {"group", "supergroup"}
        else None
    )
    success, err = await deliver_via_relay(
        destination_chat_id,
        r["chat"],
        r["msg_id"],
        ctx.bot,
        reply_to_message_id,
    )
    if not success:
        await q.message.reply_text(GENERIC_ERROR_MESSAGE)


async def handle_error(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """يسجل التفاصيل داخليًا ولا يكشفها للمستخدم."""
    logger.error("Unhandled update error", exc_info=ctx.error)
    chat = getattr(update, "effective_chat", None)
    if not is_allowed_group(chat):
        return
    try:
        if getattr(update, "callback_query", None):
            await update.callback_query.answer(
                GENERIC_ERROR_MESSAGE,
                show_alert=True,
            )
        elif getattr(update, "message", None):
            await update.message.reply_text(GENERIC_ERROR_MESSAGE)
    except Exception:
        logger.error("Could not send generic error message", exc_info=True)


# -- التشغيل --------------------------------------------------------
def main() -> None:
    persistence = PicklePersistence(filepath="bot_persistence.pkl")
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .persistence(persistence)
        .concurrent_updates(UPDATE_WORKERS)
        .post_init(start_pyro)
        .post_shutdown(stop_pyro)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(cb_page,      pattern="^pg:"))
    app.add_handler(CallbackQueryHandler(cb_send_book, pattern="^sb:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_search))
    app.add_error_handler(handle_error)

    logger.info("Bot started...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
