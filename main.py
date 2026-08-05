import os
import re
import json
import sqlite3
import asyncio
import threading
import urllib.parse
import urllib.request
from datetime import datetime, time as dtime, timedelta, timezone

import feedparser
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# ==================== الإعدادات ====================
TOKEN = os.environ["BOT_TOKEN"]
CHANNEL_ID = os.environ.get("CHANNEL_ID", "")
WEBAPP_URL = os.environ.get("WEBAPP_URL", "")
INTERVAL_MINUTES = int(os.environ.get("INTERVAL_MINUTES", "3"))
X_BEARER_TOKEN = os.environ.get("X_BEARER_TOKEN", "")
DB_PATH = os.environ.get("DB_PATH", "news.db")

# لغة النشر في القناة (مستقلة عن لغة كل مستخدم)
CHANNEL_LANG = os.environ.get("CHANNEL_LANG", "ar")
# محادثتك الشخصية لاستقبال منبّه الإحصاءات (رقم من @userinfobot)
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")
# ساعة المنبّه اليومي بتوقيت UTC (21 = 22:00 بتوقيت الجزائر)
STATS_HOUR_UTC = int(os.environ.get("STATS_HOUR_UTC", "21"))
# قنوات يوتيوب: معرّفات تبدأ بـ UC... مفصولة بفواصل
YOUTUBE_CHANNEL_IDS = [c.strip() for c in
                       os.environ.get("YOUTUBE_CHANNEL_IDS", "").split(",") if c.strip()]

LANGS = ["ar", "en", "fr", "de", "es"]
DEFAULT_LANG = os.environ.get("DEFAULT_LANG", "ar")

# ---- أكبر مصادر أخبار التشفير ----
FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://www.theblock.co/rss.xml",
    "https://decrypt.co/feed",
    "https://beincrypto.com/feed/",
    "https://cryptoslate.com/feed/",
    "https://blockworks.co/feed",
    "https://www.newsbtc.com/feed/",
    "https://coingape.com/feed/",
    "https://u.today/rss",
    "https://ambcrypto.com/feed/",
    "https://cryptopotato.com/feed/",
    "https://news.bitcoin.com/feed/",
    "https://bitcoinmagazine.com/feed",
]

# ---- جسور RSS لأخبار X (المسار ب) ----
X_RSS_FEEDS = [
    # "https://rsshub.app/twitter/user/MinerUpdate",
    # "https://rss.app/feeds/XXXXXXXX.xml",
]

# ---- بحث X عن التعدين والعملات الرقمية (المسار أ) ----
X_QUERY = (
    '("crypto mining" OR "bitcoin mining" OR "mining bot" OR "mining app" '
    'OR hashrate OR ASIC OR "tap to earn" OR "mining rig" OR "cloud mining") '
    '-is:retweet -is:reply (lang:en OR lang:ar)'
)

# ==================== التصنيف ====================
MINING_WORDS = [
    "mining", "miner", "miners", "hashrate", "hash rate", "asic", "mining bot",
    "mining app", "mini app", "clicker", "tap to earn", "tap-to-earn",
    "telegram mining", "cloud mining", "mining rig", "airdrop", "proof of work",
]
TRADING_WORDS = [
    "trading", "trader", "futures", "spot", "leverage", "liquidation", "exchange",
    "binance", "bybit", "okx", "coinbase", "price analysis", "technical analysis",
    "etf", "rally", "pump", "dump", "market", "bullish", "bearish",
]
GENERAL_WORDS = [
    "bitcoin", "btc", "ethereum", "eth", "crypto", "altcoin", "solana", "ton",
    "blockchain", "stablecoin", "defi", "token", "sec", "regulation", "halving",
]
BLOCK_WORDS = ["sponsored", "press release", "casino", "gambling", "betting", "giveaway"]

CAT_MINING, CAT_TRADING, CAT_GENERAL = "mining", "trading", "general"


def classify(text: str):
    t = text.lower()
    if any(w in t for w in BLOCK_WORDS):
        return None
    if any(w in t for w in MINING_WORDS):
        return CAT_MINING
    if any(w in t for w in TRADING_WORDS):
        return CAT_TRADING
    if any(w in t for w in GENERAL_WORDS):
        return CAT_GENERAL
    return None


# ==================== الترجمة ====================
_TR_CACHE = {}


def translate(text: str, target: str) -> str:
    """ترجمة خفيفة مع تخزين مؤقت. تُعيد النص الأصلي عند أي خطأ."""
    if not text or target == "en":
        return text
    key = (target, text)
    if key in _TR_CACHE:
        return _TR_CACHE[key]
    try:
        url = ("https://translate.googleapis.com/translate_a/single"
               "?client=gtx&sl=auto&tl=" + target + "&dt=t&q="
               + urllib.parse.quote(text))
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode("utf-8"))
        out = "".join(seg[0] for seg in data[0] if seg and seg[0])
    except Exception:
        out = text
    if len(_TR_CACHE) > 4000:
        _TR_CACHE.clear()
    _TR_CACHE[key] = out
    return out


# ==================== قاعدة البيانات ====================
def db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.execute("""CREATE TABLE IF NOT EXISTS news(
        link TEXT PRIMARY KEY, title TEXT, source TEXT, origin TEXT,
        category TEXT, created TEXT)""")
    con.execute("""CREATE TABLE IF NOT EXISTS subs(
        chat_id INTEGER PRIMARY KEY, lang TEXT DEFAULT 'ar')""")
    con.execute("""CREATE TABLE IF NOT EXISTS sessions(
        sid TEXT PRIMARY KEY, user_id TEXT, username TEXT, lang TEXT,
        started TEXT, last_seen TEXT, seconds INTEGER DEFAULT 0)""")
    con.commit()
    return con

CON = db()


def save_news(link, title, source, origin, category):
    try:
        CON.execute("INSERT INTO news VALUES (?,?,?,?,?,?)",
                    (link, title, source, origin, category,
                     datetime.now(timezone.utc).isoformat()))
        CON.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def latest(limit=50, category=None, origin=None):
    q = "SELECT title, link, source, origin, category, created FROM news WHERE 1=1"
    p = []
    if category:
        q += " AND category = ?"; p.append(category)
    if origin:
        q += " AND origin = ?"; p.append(origin)
    q += " ORDER BY created DESC LIMIT ?"; p.append(limit)
    return [{"title": r[0], "link": r[1], "source": r[2], "origin": r[3],
             "category": r[4], "date": r[5]}
            for r in CON.execute(q, p).fetchall()]


def get_lang(chat_id):
    row = CON.execute("SELECT lang FROM subs WHERE chat_id = ?", (chat_id,)).fetchone()
    return row[0] if row and row[0] in LANGS else DEFAULT_LANG


def set_lang(chat_id, lang):
    CON.execute("INSERT INTO subs(chat_id, lang) VALUES (?,?) "
                "ON CONFLICT(chat_id) DO UPDATE SET lang = excluded.lang",
                (chat_id, lang))
    CON.commit()


# ==================== جمع الأخبار ====================
def collect_rss():
    fresh = []
    for url in FEEDS + X_RSS_FEEDS:
        origin = "x" if url in X_RSS_FEEDS else "news"
        try:
            feed = feedparser.parse(url)
            source = feed.feed.get("title", url)[:60]
            for e in feed.entries[:25]:
                title = re.sub(r"<[^>]+>", "", e.get("title", "")).strip()
                link = e.get("link", "")
                summary = re.sub(r"<[^>]+>", "", e.get("summary", ""))[:300]
                if not title or not link:
                    continue
                cat = classify(title + " " + summary)
                if origin == "x" and cat is None:
                    continue
                if cat is None:
                    continue
                if save_news(link, title, source, origin, cat):
                    fresh.append({"title": title, "link": link, "source": source,
                                  "origin": origin, "category": cat})
        except Exception as err:
            print("feed error:", url, err)
    return fresh


def collect_x():
    """أخبار لحظية من X عبر الواجهة الرسمية."""
    if not X_BEARER_TOKEN:
        return []
    fresh = []
    try:
        url = ("https://api.x.com/2/tweets/search/recent?query="
               + urllib.parse.quote(X_QUERY)
               + "&max_results=25&tweet.fields=created_at&expansions=author_id"
               "&user.fields=username,verified")
        req = urllib.request.Request(
            url, headers={"Authorization": "Bearer " + X_BEARER_TOKEN})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8"))
        users = {u["id"]: u["username"]
                 for u in data.get("includes", {}).get("users", [])}
        for t in data.get("data", []):
            text = re.sub(r"\s+", " ", t.get("text", "")).strip()
            user = users.get(t.get("author_id", ""), "i/web")
            link = "https://x.com/" + user + "/status/" + t["id"]
            cat = classify(text) or CAT_MINING
            if save_news(link, text[:220], "X · @" + user, "x", cat):
                fresh.append({"title": text[:220], "link": link,
                              "source": "X · @" + user, "origin": "x",
                              "category": cat})
    except Exception as err:
        print("x error:", err)
    return fresh


def collect_youtube():
    """أحدث فيديوهات قنوات يوتيوب عبر RSS الرسمي — بدون مفتاح API."""
    fresh = []
    for cid in YOUTUBE_CHANNEL_IDS:
        url = "https://www.youtube.com/feeds/videos.xml?channel_id=" + cid
        try:
            feed = feedparser.parse(url)
            source = "YouTube · " + feed.feed.get("title", cid)[:40]
            for e in feed.entries[:10]:
                title = e.get("title", "").strip()
                link = e.get("link", "")
                if not title or not link:
                    continue
                # قناتك تُنشر بالكامل بدون تصفية كلمات
                cat = classify(title) or CAT_GENERAL
                if save_news(link, title, source, "yt", cat):
                    fresh.append({"title": title, "link": link, "source": source,
                                  "origin": "yt", "category": cat})
        except Exception as err:
            print("youtube error:", cid, err)
    return fresh


def collect_all():
    return collect_rss() + collect_x() + collect_youtube()


# ==================== نصوص البوت ====================
T = {
    "welcome": {
        "ar": "✅ تم تشغيل الاشتراك التلقائي.\nستصلك أخبار الكريبتو والتعدين لحظيًا كل {m} دقائق.",
        "en": "✅ Auto-subscription enabled.\nYou'll receive live crypto & mining news every {m} minutes.",
        "fr": "✅ Abonnement automatique activé.\nActualités crypto et minage toutes les {m} minutes.",
        "de": "✅ Automatisches Abo aktiviert.\nKrypto- und Mining-News alle {m} Minuten.",
        "es": "✅ Suscripción automática activada.\nNoticias de cripto y minería cada {m} minutos.",
    },
    "open_app": {"ar": "📱 افتح التطبيق", "en": "📱 Open app", "fr": "📱 Ouvrir l'app",
                 "de": "📱 App öffnen", "es": "📱 Abrir la app"},
    "mining": {"ar": "⛏️ التعدين", "en": "⛏️ Mining", "fr": "⛏️ Minage",
               "de": "⛏️ Mining", "es": "⛏️ Minería"},
    "trading": {"ar": "📈 التداول", "en": "📈 Trading", "fr": "📈 Trading",
                "de": "📈 Trading", "es": "📈 Trading"},
    "x_news": {"ar": "🐦 أخبار X", "en": "🐦 X news", "fr": "🐦 Actus X",
               "de": "🐦 X-News", "es": "🐦 Noticias X"},
    "lang_set": {"ar": "🌍 تم تعيين اللغة: العربية", "en": "🌍 Language set: English",
                 "fr": "🌍 Langue définie : Français", "de": "🌍 Sprache: Deutsch",
                 "es": "🌍 Idioma: Español"},
    "empty": {"ar": "لا توجد أخبار بعد، انتظر الدورة القادمة.",
              "en": "No news yet, wait for the next cycle.",
              "fr": "Pas encore d'actualités.", "de": "Noch keine News.",
              "es": "Aún no hay noticias."},
}

FLAGS = {"ar": "🇸🇦 العربية", "en": "🇬🇧 English", "fr": "🇫🇷 Français",
         "de": "🇩🇪 Deutsch", "es": "🇪🇸 Español"}


def t(key, lang, **kw):
    s = T[key].get(lang, T[key]["en"])
    return s.format(**kw) if kw else s


def main_kb(lang):
    rows = []
    if WEBAPP_URL:
        rows.append([InlineKeyboardButton(
            t("open_app", lang),
            web_app=WebAppInfo(url=WEBAPP_URL + "?lang=" + lang))])
    rows.append([InlineKeyboardButton(t("mining", lang), callback_data="c:mining"),
                 InlineKeyboardButton(t("trading", lang), callback_data="c:trading")])
    rows.append([InlineKeyboardButton(t("x_news", lang), callback_data="c:x")])
    rows.append([InlineKeyboardButton(FLAGS[l], callback_data="l:" + l)
                 for l in LANGS])
    return InlineKeyboardMarkup(rows)


# ==================== الأوامر ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat.id
    code = (update.effective_user.language_code or DEFAULT_LANG)[:2]
    lang = code if code in LANGS else DEFAULT_LANG
    set_lang(chat, lang)
    await update.message.reply_text(
        t("welcome", lang, m=INTERVAL_MINUTES), reply_markup=main_kb(lang))


async def lang_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = get_lang(update.effective_chat.id)
    await update.message.reply_text(
        "🌍 " + " / ".join(FLAGS.values()),
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton(FLAGS[l], callback_data="l:" + l)] for l in LANGS]))


def render(items, lang):
    lines = []
    for i in items:
        title = translate(i["title"], lang)
        tag = "🐦" if i["origin"] == "x" else "📰"
        lines.append(tag + " <b>" + title + "</b>\n" + i["link"])
    return "\n\n".join(lines)


async def show(update_or_q, lang, category=None, origin=None):
    items = latest(10, category, origin)
    text = render(items, lang) if items else t("empty", lang)
    if hasattr(update_or_q, "message") and update_or_q.message:
        await update_or_q.message.reply_text(
            text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


async def mining_cmd(u, c):
    await show(u, get_lang(u.effective_chat.id), CAT_MINING)

async def trading_cmd(u, c):
    await show(u, get_lang(u.effective_chat.id), CAT_TRADING)

async def x_cmd(u, c):
    await show(u, get_lang(u.effective_chat.id), None, "x")

async def latest_cmd(u, c):
    await show(u, get_lang(u.effective_chat.id))


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    CON.execute("DELETE FROM subs WHERE chat_id = ?", (update.effective_chat.id,))
    CON.commit()
    await update.message.reply_text("⏹️ /start")


# ==================== إحصاءات الـ Mini App ====================
def fmt_dur(seconds):
    s = int(seconds or 0)
    return str(s // 60) + "د " + str(s % 60) + "ث"


def stats_text(hours=24):
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    sessions, users, total, avg, best = CON.execute(
        "SELECT COUNT(*), COUNT(DISTINCT user_id), COALESCE(SUM(seconds),0), "
        "COALESCE(AVG(seconds),0), COALESCE(MAX(seconds),0) "
        "FROM sessions WHERE started >= ?", (since,)).fetchone()
    all_users = CON.execute(
        "SELECT COUNT(DISTINCT user_id) FROM sessions").fetchone()[0]
    live = CON.execute(
        "SELECT COUNT(*) FROM sessions WHERE last_seen >= ?",
        ((datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat(),)
    ).fetchone()[0]
    return ("📊 <b>إحصاءات Mini App — آخر " + str(hours) + " ساعة</b>\n\n"
            "👥 مستخدمون فريدون: <b>" + str(users) + "</b>\n"
            "🔓 عدد الجلسات: <b>" + str(sessions) + "</b>\n"
            "⏱️ متوسط مدة الجلسة: <b>" + fmt_dur(avg) + "</b>\n"
            "🏆 أطول جلسة: <b>" + fmt_dur(best) + "</b>\n"
            "🧮 إجمالي الوقت: <b>" + fmt_dur(total) + "</b>\n"
            "🟢 متصل الآن: <b>" + str(live) + "</b>\n"
            "🌐 إجمالي المستخدمين (كل الوقت): <b>" + str(all_users) + "</b>")


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(stats_text(24), parse_mode=ParseMode.HTML)


async def job_stats(context: ContextTypes.DEFAULT_TYPE):
    """منبّه يومي بإحصاءات الـ Mini App."""
    target = ADMIN_CHAT_ID
    if not target:
        row = CON.execute("SELECT chat_id FROM subs LIMIT 1").fetchone()
        target = row[0] if row else None
    if not target:
        return
    try:
        await context.bot.send_message(target, "🔔 " + stats_text(24),
                                       parse_mode=ParseMode.HTML)
    except Exception as err:
        print("stats error:", err)


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    chat = q.message.chat_id
    data = q.data or ""
    if data.startswith("l:"):
        lang = data[2:]
        set_lang(chat, lang)
        await q.message.reply_text(t("lang_set", lang), reply_markup=main_kb(lang))
        return
    lang = get_lang(chat)
    key = data[2:]
    items = (latest(10, None, "x") if key == "x" else latest(10, key))
    await q.message.reply_text(
        render(items, lang) if items else t("empty", lang),
        parse_mode=ParseMode.HTML, disable_web_page_preview=True)


# ==================== الدورة التلقائية ====================
async def job_push(context: ContextTypes.DEFAULT_TYPE):
    fresh = await asyncio.to_thread(collect_all)
    if not fresh:
        return
    subs = CON.execute("SELECT chat_id, lang FROM subs").fetchall()
    for item in fresh[:8]:
        for chat_id, lang in subs:
            lang = lang if lang in LANGS else DEFAULT_LANG
            title = await asyncio.to_thread(translate, item["title"], lang)
            tag = "🐦 X" if item["origin"] == "x" else "📰"
            msg = (tag + "\n\n<b>" + title + "</b>\n<i>" + item["source"]
                   + "</i>\n" + item["link"])
            try:
                await context.bot.send_message(chat_id, msg,
                                               parse_mode=ParseMode.HTML)
            except Exception as err:
                print("send error:", chat_id, err)
            await asyncio.sleep(0.4)
        if CHANNEL_ID:
            try:
                # القناة تُنشر بلغة CHANNEL_LANG (العربية افتراضيًا)
                ch_title = await asyncio.to_thread(
                    translate, item["title"], CHANNEL_LANG)
                tag = ("🐦 X" if item["origin"] == "x"
                       else "▶️ يوتيوب" if item["origin"] == "yt" else "📰")
                await context.bot.send_message(
                    CHANNEL_ID,
                    tag + "\n\n<b>" + ch_title + "</b>\n<i>" + item["source"]
                    + "</i>\n" + item["link"], parse_mode=ParseMode.HTML)
            except Exception as err:
                print("channel error:", err)


# ==================== خادم الـ Mini App ====================
api = FastAPI()


@api.get("/api/news")
def api_news(category: str = "", origin: str = "", lang: str = "en", limit: int = 50):
    lang = lang if lang in LANGS else "en"
    items = latest(limit, category or None, origin or None)
    for i in items:
        i["title"] = translate(i["title"], lang)
    return JSONResponse({"server_time": datetime.now(timezone.utc).isoformat(),
                         "items": items})


@api.post("/api/track")
async def api_track(req: Request):
    """يستقبل نبضات الجلسة من الـ Mini App (بدء / نبضة / نهاية)."""
    try:
        body = await req.json()
    except Exception:
        return {"ok": False}
    sid = str(body.get("sid", ""))[:64]
    if not sid:
        return {"ok": False}
    now = datetime.now(timezone.utc).isoformat()
    seconds = int(body.get("seconds") or 0)
    if body.get("event") == "start":
        CON.execute(
            "INSERT OR IGNORE INTO sessions(sid,user_id,username,lang,"
            "started,last_seen,seconds) VALUES (?,?,?,?,?,?,0)",
            (sid, str(body.get("user_id", "anon"))[:32],
             str(body.get("username", ""))[:64],
             str(body.get("lang", ""))[:5], now, now))
    else:
        CON.execute(
            "UPDATE sessions SET last_seen = ?, seconds = MAX(seconds, ?) "
            "WHERE sid = ?", (now, seconds, sid))
    CON.commit()
    return {"ok": True}


@api.get("/api/stats")
def api_stats(hours: int = 24):
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    s, u, total, avg, best = CON.execute(
        "SELECT COUNT(*), COUNT(DISTINCT user_id), COALESCE(SUM(seconds),0), "
        "COALESCE(AVG(seconds),0), COALESCE(MAX(seconds),0) "
        "FROM sessions WHERE started >= ?", (since,)).fetchone()
    return {"hours": hours, "sessions": s, "unique_users": u,
            "total_seconds": int(total), "avg_seconds": round(avg or 0, 1),
            "max_seconds": int(best)}


@api.get("/app", response_class=HTMLResponse)
def app_page():
    with open("webapp.html", encoding="utf-8") as f:
        return f.read()


@api.get("/")
def health():
    return {"status": "ok", "feeds": len(FEEDS), "x_api": bool(X_BEARER_TOKEN)}


def run_api():
    uvicorn.run(api, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))


# ==================== الإقلاع ====================
def main():
    threading.Thread(target=run_api, daemon=True).start()

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("lang", lang_cmd))
    app.add_handler(CommandHandler("mining", mining_cmd))
    app.add_handler(CommandHandler("trading", trading_cmd))
    app.add_handler(CommandHandler("x", x_cmd))
    app.add_handler(CommandHandler("latest", latest_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CallbackQueryHandler(on_button))

    app.job_queue.run_repeating(job_push, interval=INTERVAL_MINUTES * 60, first=10)
    app.job_queue.run_daily(
        job_stats, time=dtime(hour=STATS_HOUR_UTC, minute=0, tzinfo=timezone.utc))
    app.run_polling()


if __name__ == "__main__":
    main()
